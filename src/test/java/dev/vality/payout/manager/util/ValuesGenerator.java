package dev.vality.payout.manager.util;

import java.util.UUID;

public class ValuesGenerator {

    public static String generatePayoutId() {
        return UUID.randomUUID().toString();
    }
}
