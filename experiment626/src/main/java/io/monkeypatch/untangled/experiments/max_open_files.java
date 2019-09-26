package io.monkeypatch.untangled.experiments;
import java.io.*;
import java.util.*;

public class max_open_files {
    public static void main(String ... args) throws Exception {
        File testDir = new File("/tmp/tempsubdir");
        testDir.mkdirs();

        List<File> files = new LinkedList<File>();
        List<RandomAccessFile> fileHandles = new LinkedList<RandomAccessFile>();

        try {
            while (true) {
                File f = new File(testDir, "tmp" + fileHandles.size());
                RandomAccessFile raf = new RandomAccessFile(f, "rw");
                files.add(f);
                fileHandles.add(raf);
            }
        } catch (Exception ex) {
            System.out.println(ex.getClass() + " " + ex.getMessage());
        }

        for (RandomAccessFile raf : fileHandles) raf.close();

        for (File f : files) f.delete();

        System.out.println("max open files: " + fileHandles.size());
    }
}