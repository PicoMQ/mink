locals {
  cache_dir = "/cache"

  external_catalog_settings = var.external_catalog == null ? {} : {
    for key, value in {
      URI           = var.external_catalog.uri
      WAREHOUSE     = var.external_catalog.warehouse
      AUTH__TYPE    = try(var.external_catalog.auth.type, null)
      AUTH__SERVICE = try(var.external_catalog.auth.service, null)
      AUTH__REGION  = try(var.external_catalog.auth.region, null)
    } : "MINK_EXTERNAL_CATALOGS__LAKE__${key}" => value if value != null
  }

  kafka_security_protocol = "${var.use_kafka_sasl ? "SASL_" : ""}${var.kafka.tls ? "SSL" : "PLAINTEXT"}"
  kafka_login_modules = {
    PLAIN           = "org.apache.kafka.common.security.plain.PlainLoginModule"
    "SCRAM-SHA-256" = "org.apache.kafka.common.security.scram.ScramLoginModule"
    "SCRAM-SHA-512" = "org.apache.kafka.common.security.scram.ScramLoginModule"
  }
}
