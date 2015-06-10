package edu.berkeley.cs.succinctgraph.neo4jbench.scratch;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TestMMap {

    public static void main(String[] args) {
        try {
            File file = new File(args[0]);
            FileChannel channel = new RandomAccessFile(file, "r").getChannel();
            ByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0L, file.length());
            for (int i = 0; i < file.length(); ++i)
                System.out.printf("data = %x\n", buf.getInt(i));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
