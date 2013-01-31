package edu.berkeley.thebes.app.cli;

import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.hat.client.ThebesHATClient;

import java.io.Console;
import java.nio.ByteBuffer;

public class ThebesCLIApp {

    private static void printUsage() {
        System.out.println("Invalid command: s(tart), g(et), p(ut), e(nd), q(uit)");
    }

    private static void doExit() {
        System.out.println("Thanks for playing!");
        System.exit(0);
    }


    public static void main(String[] args) {
        try {
            IThebesClient client = new ThebesHATClient();
            client.open(args);

            while(true) {
                String command = System.console().readLine("> ");
                if(command == null)
                    doExit();

                String[] command_args = command.split(" ");
                if(command_args.length == 1) {
                    if(command_args[0].equals("q") || command_args[0].equals("quit"))
                        doExit();
                    else if(command_args[0].equals("s") || command_args[0].equals("start"))
                        client.beginTransaction();
                    else if(command_args[0].equals("e") || command_args[0].equals("end"))
                        client.endTransaction();
                    else
                        printUsage();
                }
                else if(command_args.length == 2) {
                    if(command_args[0].equals("g") || command_args[0].equals("get"))
                        System.out.println(new String(client.get(command_args[1]).array()));
                    else
                        printUsage();
                }
                else if(command_args.length == 3) {
                    if(command_args[0].equals("p") || command_args[0].equals("put"))
                        System.out.println(client.put(command_args[1], ByteBuffer.wrap(command_args[2].getBytes())));
                    else
                        printUsage();
                }
                else
                    printUsage();
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}