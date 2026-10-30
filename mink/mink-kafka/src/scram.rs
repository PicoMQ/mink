use std::io;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256, Sha512};

use crate::Error;

const MIN_ITERATIONS: u32 = 4096;
const NONCE_BYTES: usize = 24;
const GS2_HEADER: &str = "n,,";
const CHANNEL_BINDING: &str = "biws";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hash {
    Sha256,
    Sha512,
}

pub(crate) struct Scram {
    hash: Hash,
    nonce: String,
    first_bare: String,
}

pub(crate) struct ServerSignature {
    hash: Hash,
    server_key: Vec<u8>,
    auth_message: String,
}

impl Scram {
    pub(crate) fn new(hash: Hash, username: &str) -> Result<Scram, Error> {
        let mut nonce = [0; NONCE_BYTES];
        getrandom::fill(&mut nonce).map_err(io::Error::other)?;

        Ok(Scram::with_nonce(hash, username, STANDARD.encode(nonce)))
    }

    fn with_nonce(hash: Hash, username: &str, nonce: String) -> Scram {
        let username = username.replace('=', "=3D").replace(',', "=2C");
        let first_bare = format!("n={username},r={nonce}");

        Scram {
            hash,
            nonce,
            first_bare,
        }
    }

    pub(crate) fn first_message(&self) -> String {
        format!("{GS2_HEADER}{}", self.first_bare)
    }

    pub(crate) fn final_message(
        &self,
        password: &str,
        server_first: &str,
    ) -> Result<(String, ServerSignature), Error> {
        let nonce = attribute(server_first, 'r').ok_or(Error::Scram("no nonce"))?;
        let salt = attribute(server_first, 's').ok_or(Error::Scram("no salt"))?;
        let iterations = attribute(server_first, 'i').ok_or(Error::Scram("no iterations"))?;

        if !nonce.starts_with(&self.nonce) || nonce.len() == self.nonce.len() {
            return Err(Error::Scram(
                "the server nonce does not extend the client nonce",
            ));
        }
        let salt = STANDARD
            .decode(salt)
            .map_err(|_| Error::Scram("the salt is not base64"))?;
        let iterations: u32 = iterations
            .parse()
            .map_err(|_| Error::Scram("the iteration count is not a number"))?;
        if iterations < MIN_ITERATIONS {
            return Err(Error::Scram("the iteration count is below 4096"));
        }

        let without_proof = format!("c={CHANNEL_BINDING},r={nonce}");
        let auth_message = format!("{},{server_first},{without_proof}", self.first_bare);
        let salted = salted_password(self.hash, password.as_bytes(), &salt, iterations);
        let client_key = hmac(self.hash, &salted, b"Client Key");
        let stored_key = digest(self.hash, &client_key);
        let client_signature = hmac(self.hash, &stored_key, auth_message.as_bytes());

        let proof: Vec<u8> = client_key
            .iter()
            .zip(&client_signature)
            .map(|(key, signature)| key ^ signature)
            .collect();
        let message = format!("{without_proof},p={}", STANDARD.encode(proof));
        let signature = ServerSignature {
            hash: self.hash,
            server_key: hmac(self.hash, &salted, b"Server Key"),
            auth_message,
        };

        Ok((message, signature))
    }
}

impl ServerSignature {
    pub(crate) fn verify(&self, server_final: &str) -> Result<(), Error> {
        if let Some(error) = attribute(server_final, 'e') {
            return Err(Error::Authentication(error.to_owned()));
        }

        let signature = attribute(server_final, 'v').ok_or(Error::Scram("no server signature"))?;
        let signature = STANDARD
            .decode(signature)
            .map_err(|_| Error::Scram("the server signature is not base64"))?;

        match verify(
            self.hash,
            &self.server_key,
            self.auth_message.as_bytes(),
            &signature,
        ) {
            true => Ok(()),
            false => Err(Error::Scram("the server signature does not match")),
        }
    }
}

fn attribute(message: &str, name: char) -> Option<&str> {
    message
        .split(',')
        .find_map(|part| part.strip_prefix(name)?.strip_prefix('='))
}

fn salted_password(hash: Hash, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let first = [salt, &1u32.to_be_bytes()].concat();
    let mut previous = hmac(hash, password, &first);
    let mut salted = previous.clone();

    for _ in 1..iterations {
        previous = hmac(hash, password, &previous);
        salted
            .iter_mut()
            .zip(&previous)
            .for_each(|(byte, next)| *byte ^= next);
    }

    salted
}

fn hmac(hash: Hash, key: &[u8], data: &[u8]) -> Vec<u8> {
    match hash {
        Hash::Sha256 => mac::<Hmac<Sha256>>(key, data)
            .finalize()
            .into_bytes()
            .to_vec(),
        Hash::Sha512 => mac::<Hmac<Sha512>>(key, data)
            .finalize()
            .into_bytes()
            .to_vec(),
    }
}

fn verify(hash: Hash, key: &[u8], data: &[u8], expected: &[u8]) -> bool {
    match hash {
        Hash::Sha256 => mac::<Hmac<Sha256>>(key, data)
            .verify_slice(expected)
            .is_ok(),
        Hash::Sha512 => mac::<Hmac<Sha512>>(key, data)
            .verify_slice(expected)
            .is_ok(),
    }
}

fn mac<M: Mac + KeyInit>(key: &[u8], data: &[u8]) -> M {
    let mut mac = <M as KeyInit>::new_from_slice(key).expect("hmac takes keys of any length");
    mac.update(data);

    mac
}

fn digest(hash: Hash, data: &[u8]) -> Vec<u8> {
    match hash {
        Hash::Sha256 => Sha256::digest(data).to_vec(),
        Hash::Sha512 => Sha512::digest(data).to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLIENT_NONCE: &str = "rOprNGfwEbeRWgbNEkqO";
    const SERVER_FIRST: &str = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                                s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

    fn rfc7677() -> Scram {
        Scram::with_nonce(Hash::Sha256, "user", CLIENT_NONCE.to_owned())
    }

    #[test]
    fn the_exchange_matches_the_rfc_7677_example() {
        let scram = rfc7677();

        let (message, signature) = scram.final_message("pencil", SERVER_FIRST).unwrap();

        assert_eq!(scram.first_message(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        assert_eq!(
            message,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        signature
            .verify("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
    }

    #[test]
    fn a_wrong_server_signature_is_rejected() {
        let (_, signature) = rfc7677().final_message("pencil", SERVER_FIRST).unwrap();

        let verified = signature.verify("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");

        assert!(matches!(verified, Err(Error::Scram(_))), "{verified:?}");
    }

    #[test]
    fn a_server_error_is_an_authentication_failure() {
        let (_, signature) = rfc7677().final_message("pencil", SERVER_FIRST).unwrap();

        let verified = signature.verify("e=invalid-proof");

        assert!(
            matches!(&verified, Err(Error::Authentication(message)) if message == "invalid-proof"),
            "{verified:?}"
        );
    }

    #[test]
    fn a_nonce_that_does_not_extend_the_client_nonce_is_rejected() {
        let scram = rfc7677();
        let other = "r=somebodyElse123,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let same = "r=rOprNGfwEbeRWgbNEkqO,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

        for server_first in [other, same] {
            let result = scram.final_message("pencil", server_first);

            assert!(matches!(result, Err(Error::Scram(_))));
        }
    }

    #[test]
    fn fewer_than_4096_iterations_are_rejected() {
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvY,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4095";

        let result = rfc7677().final_message("pencil", server_first);

        assert!(
            matches!(
                result,
                Err(Error::Scram("the iteration count is below 4096"))
            ),
            "{:?}",
            result.err()
        );
    }

    #[test]
    fn usernames_escape_equals_and_comma() {
        let scram = Scram::with_nonce(Hash::Sha512, "a=b,c", "nonce".to_owned());

        assert_eq!(scram.first_message(), "n,,n=a=3Db=2Cc,r=nonce");
    }
}
