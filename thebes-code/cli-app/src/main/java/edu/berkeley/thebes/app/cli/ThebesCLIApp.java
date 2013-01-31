package edu.berkeley.thebes.app.cli;

import edu.berkeley.thebes.client.ThebesClient;
import edu.berkeley.thebes.common.interfaces.IThebesClient;

import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

public class ThebesCLIApp {

    private static Console console;
    private static String[] inputCommands;
    private static int currentPosition = 0;

    private static void printUsage() {
        System.out.println("Invalid command: s(tart), g(et), p(ut), e(nd), q(uit)");
    }

    private static void doExit() {
        System.out.println("Thanks for playing!");
        System.exit(0);
    }

    private static String getNextCommand() {
        String ret;
        if(console != null) {
            ret = console.readLine("> ");
        }
        else {
            if(currentPosition < inputCommands.length)
                ret = inputCommands[currentPosition];
            else
                ret = "q";
            currentPosition++;
        }

        return ret;
    }


    public static void main(String[] args) {
        try {
            IThebesClient client = new ThebesClient();
            client.open(args);

            console = System.console();
            if(console == null)
                inputCommands = new BufferedReader(new InputStreamReader(System.in)).readLine().split(";");


            while(true) {
                String command = getNextCommand();
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
                        System.out.printf("GET %s -> '%s'\n", command_args[1],
                                                              new String(client.get(command_args[1]).array()));
                    else
                        printUsage();
                }
                else if(command_args.length == 3) {
                    if(command_args[0].equals("p") || command_args[0].equals("put"))
                        System.out.printf("PUT %s='%s' -> %b\n", command_args[1],
                                                                 command_args[2],
                                                                 client.put(command_args[1],
                                                                            ByteBuffer.wrap(command_args[2]
                                                                                                    .getBytes())));
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