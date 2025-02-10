package org.raj.sample;

import org.apache.commons.cli.CommandLine;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CommandLineParserUtilTest {

    @Test
    public void testParseArguments_ValidInput() {
        String[] args = {"-i", "input", "-o", "output", "-t", "1 hour"};
        CommandLine cmd = CommandLineParserUtil.parseArguments(args);

        assertNotNull(cmd);
        assertEquals("input", cmd.getOptionValue("i"));
        assertEquals("output", cmd.getOptionValue("o"));
        assertEquals("1 hour", cmd.getOptionValue("t"));
    }

    @Test
    public void testParseArgumentsMissingRequiredArguments() {
        String[] args = {"-i", "input", "-o", "output"};
        CommandLine cmd = CommandLineParserUtil.parseArguments(args);
        assertEquals("input", cmd.getOptionValue("i"));
        assertEquals("output", cmd.getOptionValue("o"));
        assertNull(cmd.getOptionValue("t"));
    }


    @Test
    public void testParseArgumentsUnknownOption() {
        String[] args = {"-i", "input", "-o", "output", "-x", "unknown"};
        Exception exception = assertThrows(RuntimeException.class, () -> {
            CommandLineParserUtil.parseArguments(args);;
        });
        assertTrue(exception.getMessage().contains("Unrecognized option: -x"));
    }

}
