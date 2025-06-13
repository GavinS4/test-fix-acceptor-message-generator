package com.example.fixacceptor;

import quickfix.*;

public class FixAcceptorLauncher {
    public static void main(String[] args) throws Exception {
        SessionSettings settings = new SessionSettings("config/acceptor.cfg");
        Application app = new AcceptorApp();
        MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new FileLogFactory(settings);
        MessageFactory messageFactory = new DefaultMessageFactory();

        SocketAcceptor acceptor = new SocketAcceptor(app, storeFactory, settings, logFactory, messageFactory);
        acceptor.start();
        System.out.println("FIX Acceptor started...");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            acceptor.stop();
            System.out.println("FIX Acceptor stopped.");
        }));
    }
}
