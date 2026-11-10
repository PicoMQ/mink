package mink.athena;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public final class Handler extends CompositeHandler {
    public Handler() {
        super(new Metadata(System.getenv()), new Records(System.getenv()));
    }
}
