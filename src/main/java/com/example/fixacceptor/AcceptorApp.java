package com.example.fixacceptor;

import quickfix.*;
import quickfix.Message;
import quickfix.field.*;
import quickfix.field.Currency;
import quickfix.fix44.*;
import quickfix.fix44.MessageCracker;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;

public class AcceptorApp extends MessageCracker implements Application {

    private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();
    private final List<String> symbols = Arrays.asList("EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD");
    private final Random random = new Random();
    private volatile SessionID sessionID;

    @Override
    public void onCreate(SessionID sessionId) {}

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
    public void toAdmin(Message message, SessionID sessionId) {}

    @Override
    public void fromAdmin(Message message, SessionID sessionId) {}

    @Override
    public void toApp(Message message, SessionID sessionId) throws DoNotSend {}

    @Override
    public void fromApp(Message message, SessionID sessionId) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
        crack(message, sessionId);
    }

    @Override
    public void onMessage(MarketDataRequest request, SessionID sessionID) throws FieldNotFound {
        String symbol = request.getGroup(1, NoRelatedSym.FIELD).getString(Symbol.FIELD);
        subscriptions.putIfAbsent(symbol, new HashSet<>());
        for (int i = 1; i <= request.getInt(NoMDEntryTypes.FIELD); i++) {
            String entryType = request.getGroup(i, NoMDEntryTypes.FIELD).getString(MDEntryType.FIELD);
            subscriptions.get(symbol).add(entryType);
        }
        System.out.println("Subscribed to MarketData: " + symbol + " with entry types " + subscriptions.get(symbol));

        sendMarketDataSnapshot(request, sessionID, symbol);
    }

    @Override
    public void onMessage(TradeCaptureReportRequest request, SessionID sessionID) throws FieldNotFound {
        String symbol = request.getString(Symbol.FIELD);
        System.out.println("Subscribed to TradeCaptureReport: " + symbol);
        sendTradeCaptureReport(symbol);
    }

    @Override
    public void onMessage(SecurityDefinitionRequest request, SessionID sessionID) throws FieldNotFound {
        String symbol = request.getString(Symbol.FIELD);
        System.out.println("Subscribed to SecurityDefinition: " + symbol);
        sendSecurityDefinition(symbol);
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

    private void sendMarketDataSnapshot(MarketDataRequest request, SessionID sessionID, String symbol) {
        try {
            MarketDataSnapshotFullRefresh snapshot = new MarketDataSnapshotFullRefresh();
            snapshot.set(new MDReqID(request.getString(MDReqID.FIELD)));
            snapshot.set(new Symbol(symbol));

            for (int i = 1; i <= request.getInt(NoMDEntryTypes.FIELD); i++) {
                char entryType = request.getGroup(i, NoMDEntryTypes.FIELD).getChar(MDEntryType.FIELD);
                MarketDataSnapshotFullRefresh.NoMDEntries group = new MarketDataSnapshotFullRefresh.NoMDEntries();
                group.set(new MDEntryType(entryType));
                group.set(new MDEntryPx(round(random.nextDouble() * 100, 5)));
                group.set(new MDEntrySize(round(random.nextDouble() * 10, 1)));
                group.set(new MDEntryTime(new Time(System.currentTimeMillis()).toLocalTime()));
                snapshot.addGroup(group);
            }

            Session.sendToTarget(snapshot, sessionID);
            System.out.println("Sent MarketDataSnapshotFullRefresh for " + symbol);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendTradeCaptureReport(String symbol) {
        try {
            TradeCaptureReport report = new TradeCaptureReport();
            report.set(new TradeReportID("TR" + UUID.randomUUID().toString().substring(0, 6)));
            report.set(new Symbol(symbol));
            report.set(new LastPx(round(random.nextDouble() * 100, 5)));
            report.set(new LastQty(random.nextInt(500) + 1));
            report.set(new TradeDate(LocalDateTime.now().toLocalDate().toString()));
            report.set(new TransactTime(LocalDateTime.now()));

            Session.sendToTarget(report, sessionID);
            System.out.println("Sent TradeCaptureReport for " + symbol);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendSecurityDefinition(String symbol) {
        try {
            SecurityDefinition def = new SecurityDefinition();
            def.set(new SecurityReqID("SECREQ" + UUID.randomUUID().toString().substring(0, 6)));
            def.set(new SecurityResponseID("SECRESP" + UUID.randomUUID().toString().substring(0, 6)));
            def.set(new SecurityResponseType(1)); // Accepted
            def.set(new Symbol(symbol));
            def.set(new SecurityType(SecurityType.FOREIGN_EXCHANGE_CONTRACT));
            def.set(new Currency("USD"));

            Session.sendToTarget(def, sessionID);
            System.out.println("Sent SecurityDefinition for " + symbol);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private double round(double val, int places) {
        double scale = Math.pow(10, places);
        return Math.round(val * scale) / scale;
    }
}