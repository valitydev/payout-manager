package dev.vality.payout.manager.exception;

public class PayoutAlreadyExistsException extends RuntimeException {

    public PayoutAlreadyExistsException(String message) {
        super(message);
    }
}
