package kfinley.keyvaluestore;

public class ModificationFailureException extends RuntimeException {
    private final String message;

    public ModificationFailureException(String message, Exception e) {
        super(e);
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
