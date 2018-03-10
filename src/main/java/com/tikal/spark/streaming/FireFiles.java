package com.tikal.spark.streaming;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FireFiles {

    public static void main(String[] args) {
        try {
            if (args.length != 4) {
                System.out.println("Specify arguments: <dir> <num_of_lines> <mode> <time_interval_millis>");
                System.exit(-1);
            }
            Logger logger = LoggerFactory.getLogger("FireFiles");

            String inputDirStr = args[0];
            FileSystem fs = FileSystem.get(new URI(inputDirStr), new Configuration());
            logger.error("starting with folder " + inputDirStr);
            Path inputDir = new Path(inputDirStr);

            String input = null;
            long totalLines = 0;
            while(!(input = getInput(args[2])).equals("exit")) {
                String fileNum = getNextFileNumber();
                String fileName = "file-" + fileNum + ".csv";
                int numberOfLines = Integer.parseInt(args[1]);
                logger.error("Writing file " + fileName + " with " + numberOfLines + " lines. Written so far: " + totalLines);

                LocalDateTime now = LocalDateTime.now();

                if (input.equals("late")) {
                    logger.info("Late data by 5 minutes");
                    now = now.minusMinutes(5);
                }
                CSVWriter writer = openFile(fs, new Path(inputDir, fileName));
                writeStrings(fileNum, now, numberOfLines, writer);
                writer.close();
                totalLines += numberOfLines;
                logger.error("Done writing file " + fileName);
                Thread.sleep(Integer.parseInt(args[3]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeStrings(String fileNum, LocalDateTime now, int numOfLines, CSVWriter writer) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        for (int i = 0; i < numOfLines; i++) {
            int amount = (int)(Math.random() * 1000);
            String[] line = new String[]{fileNum, getNextRowNumber(), now.minusSeconds(5).format(formatter), String.valueOf(amount)};
            writer.writeNext(line);
        }
    }

    private static String getInput(String mode) throws IOException {
        if (mode.equals("auto")) {
            return "\n";
        }
        byte[] buff = new byte[1024];
        System.in.read(buff);
        String in = new String(buff).trim();
        return in;
    }

    private static String getAutoInput() {
        return "\n";
    }

    private static CSVWriter openFile(FileSystem fs, Path path) throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(fs.create(path));
        CSVWriter writer = new CSVWriter(out);
        return writer;
    }

    private static int rowNum = 0;

    private static synchronized String getNextRowNumber() {
        rowNum++;
        return String.valueOf(rowNum);
    }

    private static int fileNum = 0;

    private static synchronized String getNextFileNumber() {
        fileNum++;
        return String.valueOf(fileNum);
    }
}
