package io.monkeypatch.untangled.experiments;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static io.monkeypatch.untangled.experiments.Log.println;

/**
 * Just an experiment to show that AsynchronousFileChannel is, in fact, thread-blocking.
 * The default executor keeps creating new threads to handle the load so
 * is not an event loop like the javadoc made me think (it's tiny bit ambiguous,
 * i.e: "An AsynchronousFileChannel is associated with a thread pool to
 * which tasks are submitted to handle I/O events" <== "events" made me wonder).
 */
public class AsyncFileChannel {
    private static final int NB_FILES = 1000;

    public static void main(String[] args) throws InterruptedException, IOException {

        println("Start reading files.");

        CountDownLatch l = new CountDownLatch(NB_FILES);
        AsynchronousFileChannel[] fileChannels = new AsynchronousFileChannel[NB_FILES];
        for (int i=0;i<NB_FILES;i++) {
            try {
                Path path = Paths.get("/Users/arnaud/Lab/untangled/hawaii/src/main/java/io/monkeypatch/untangled/Chapter01_SyncBlocking.java");
                fileChannels[i] =
                    AsynchronousFileChannel.open(path, StandardOpenOption.READ);

                ByteBuffer buffer = ByteBuffer.allocate(1024);
                AtomicLong position = new AtomicLong(0);

                int finalI = i;
                println(finalI + ":: Start reading file at position = " + position.get());

                fileChannels[i].read(buffer, position.get(), buffer, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ByteBuffer attachment) {

                        if (result!=-1) {
                            position.updateAndGet(p -> p + result);
                            attachment.flip();
                            byte[] data = new byte[attachment.limit()];
                            attachment.get(data);
                            println(finalI + ":: Completed reading file, result = " + new String(data));
                            attachment.flip();
                            attachment.clear();

                            println(finalI + ":: Start reading file at position = " + position.get());
                            fileChannels[finalI].read(attachment, position.get(), attachment, this);
                        } else {
                            l.countDown();
                        }
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment) {
                        println(finalI + ":: Failed reading file, error = " + exc);
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        l.await();

        for (int i=0;i<NB_FILES;i++) {
            fileChannels[i].close();
        }
        println("Done reading files.");
    }
}
