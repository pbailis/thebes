package edu.berkeley.thebes.app.cli;

import edu.berkeley.thebes.client.ThebesClient;
import edu.berkeley.thebes.common.interfaces.IThebesClient;

import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
    
    public enum Command {
        QUIT (0, "q", "quit"),
        START (0, "s", "start"),
        END (0, "e", "end"),
        GET (1, "g", "get"),
        PUT (2, "p", "put"),
        ;
        
        private int numArgs;
        private String[] names;

        private Command(int numArgs, String ... names) {
            this.numArgs = numArgs;
            this.names = names;
        }
        
        public static Command getMatchingCommand(String name, String[] args) {
            for (Command c : Command.values()) {
                if (c.matchesCommandName(name) && c.numArgs == args.length) {
                    return c;
                }
            }
            return null;
        }
        
        public boolean matchesCommandName(String command) {
            for (String name : names) {
                if (name.equalsIgnoreCase(command)) {
                    return true;
                }
            }
            return false;
        }
    }


    public static void main(String[] args) {
        try {
            IThebesClient client = new ThebesClient();
            client.open();

            console = System.console();
            if(console == null)
                inputCommands = new BufferedReader(new InputStreamReader(System.in)).readLine().split(";");


            while(true) {
                String command = getNextCommand().trim();
                if(command == null)
                    doExit();

                String[] splitCommand = command.split(" ");
                String commandName = splitCommand[0];
                String[] commandArgs = Arrays.copyOfRange(splitCommand, 1, splitCommand.length);
                Command c = Command.getMatchingCommand(commandName, commandArgs);
                
                if (c == null) {
                    printUsage();
                } else {
                    switch (c) {
                    case QUIT:
                        doExit();
                        break;
                    case START:
                        client.beginTransaction();
                        break;
                    case END:
                        client.endTransaction();
                        break;
                        
                    case GET:
                        System.out.printf("GET %s -> '%s'\n", commandArgs[0],
                                new String(client.get(commandArgs[0]).array()));
                        break;
                    case PUT:
                        System.out.printf("PUT %s='%s' -> %b\n",
                                commandArgs[0], commandArgs[1],
                                client.put(commandArgs[0],
                                           ByteBuffer.wrap(commandArgs[1].getBytes())));
                        break;
                    }
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}