package com.example.fixacceptor;

import quickfix.*;
import quickfix.Message;
import quickfix.field.*;
import quickfix.fix44.*;
import quickfix.fix44.MessageCracker;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;

public class AcceptorApp extends MessageCracker implements Application {

    private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();
    private final List<String> symbols = Arrays.asList("EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD");
    private volatile SessionID sessionID;
    private final Random random = new Random();

    @Override
    public void onCreate(SessionID sessionId) { }

    @Override
    public void onLogon(SessionID sessionId) {
        this.sessionID = sessionId;
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::sendMessages, 1, 3, TimeUnit.SECONDS);
    }

    @Override
    public void onLogout(SessionID sessionId) {
        subscriptions.clear();
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) { }

    @Override
    public void fromAdmin(Message message, SessionID sessionId) { }

    @Override
    public void toApp(Message message, SessionID sessionId) throws DoNotSend { }

    @Override
    public void fromApp(Message message, SessionID sessionId) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
        crack(message, sessionId);
    }

    public void onMessage(MarketDataRequest message, SessionID sessionID) throws FieldNotFound {
        String symbol = message.getGroup(1, NoRelatedSym.FIELD).getString(Symbol.FIELD);
        subscriptions.putIfAbsent(symbol, new HashSet<>());
        for (int i = 1; i <= message.getInt(NoMDEntryTypes.FIELD); i++) {
            String entryType = message.getGroup(i, NoMDEntryTypes.FIELD).getString(MDEntryType.FIELD);
            subscriptions.get(symbol).add(entryType);
        }
        System.out.println("Subscribed to: " + symbol + " with entry types " + subscriptions.get(symbol));
    }

    private void sendMessages() {
        if (sessionID == null) return;
        for (String symbol : subscriptions.keySet()) {
            if (subscriptions.get(symbol).contains("0") || subscriptions.get(symbol).contains("1")) {
                sendQuote(symbol);
            }
            sendExecutionReport(symbol);
        }
    }

    private void sendQuote(String symbol) {
        try {
            Quote quote = new Quote();
            quote.set(new QuoteID(UUID.randomUUID().toString().substring(0, 8)));
            quote.set(new Symbol(symbol));
            quote.set(new BidPx(round(random.nextDouble() * 100 + 1, 5)));
            quote.set(new OfferPx(round(random.nextDouble() * 100 + 1, 5)));
            quote.set(new TransactTime(LocalDateTime.now()));
            Session.sendToTarget(quote, sessionID);
            System.out.println("Sent Quote for " + symbol);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendExecutionReport(String symbol) {
        try {
            int qty = random.nextInt(900) + 100;
            double price = round(random.nextDouble() * 100 + 1, 5);
            ExecutionReport report = new ExecutionReport(
                    new OrderID(UUID.randomUUID().toString().substring(0, 8)),
                    new ExecID(UUID.randomUUID().toString().substring(0, 8)),
                    new ExecType(ExecType.FILL),
                    new OrdStatus(OrdStatus.FILLED),
                    new Side(Side.BUY),
                    new LeavesQty(0),
                    new CumQty(qty),
                    new AvgPx(price)
            );
            report.set(new ClOrdID("CL" + UUID.randomUUID().toString().substring(0, 6)));
            report.set(new Symbol(symbol));
            report.set(new LastQty(qty));
            report.set(new LastPx(price));
            report.set(new TransactTime(LocalDateTime.now()));
            Session.sendToTarget(report, sessionID);
            System.out.println("Sent ExecutionReport for " + symbol);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private double round(double val, int places) {
        double scale = Math.pow(10, places);
        return Math.round(val * scale) / scale;
    }
}
