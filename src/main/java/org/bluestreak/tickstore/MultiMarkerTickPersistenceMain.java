/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bluestreak.tickstore;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.factory.JournalFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Overview
 * ========
 * <p/>
 * Market pub/sub prototype. This class starts up parallel market publishers, publishing to
 * disruptor ring buffer. There are two consumers, one to persist Ticks and another to calculate VWAP for
 * each of the instruments.
 * <p/>
 * Assumptions
 * ===========
 * <p/>
 * This implementation makes an assumption that instruments are zero-based integer numbers. Based on this
 * assumption the implementation is using instruments as array indexes to lessen impact on GC.
 * <p/>
 * The number of markets is limited by number of available CPUs. Running more markets than CPU-2 will slow
 * down this prototype.
 * <p/>
 * Printing is optional because it wouldn't be human-readable at 200,000,000 ticks, however
 * even if printing is disabled the string to be printed is still formatted.
 * <p/>
 * Performance
 * ===========
 * <p/>
 * This implementation is nearly zero-GC. On a I7-920@4Ghz is calculates and persists 200,000,000 ticks
 * in 32s in 32Mb heap without triggering GC.
 */
public class MultiMarkerTickPersistenceMain {

    // ring buffer size 128k
    public static final int BUFFER_SIZE = 1024 * 128;

    // directory where "tiq" database will be created.
    public static final String STORE_DIR = "c:/mynfsdb";

    // tick queue
    private final RingBuffer<Tick> ringBuffer;
    // tick store consumer
    private final BatchEventProcessor<Tick> storeProcessor;
    // tick VWAP compute consumer
    private final BatchEventProcessor<Tick> vwapProcessor;
    // thread pool
    private final ExecutorService service;
    // store consumer countdown latch
    private final CountDownLatch latch;
    private final int instrumentCount;
    private final int marketCount;
    private final int messageCount;

    /**
     * Wires up a new instance of market event publishers and market event processors.
     *
     * @param factory         journal factory of writer
     * @param instrumentCount number of instruments to be generated
     * @param marketCount     number of market publishers
     * @param messageCount    total number of messages to publish.
     */
    public MultiMarkerTickPersistenceMain(JournalFactory factory, int instrumentCount, int marketCount, int messageCount, boolean printPrices) {
        this.instrumentCount = instrumentCount;
        this.marketCount = marketCount;
        this.messageCount = messageCount;
        this.latch = new CountDownLatch(marketCount);
        this.service = Executors.newCachedThreadPool();
        // ring buffer with busy spin strategy to avoid CPU context switches.
        this.ringBuffer = RingBuffer.createMultiProducer(Tick.EVENT_FACTORY, BUFFER_SIZE, new BusySpinWaitStrategy());
        // both consumers run in parallel, so they use the same barrier
        SequenceBarrier barrier = this.ringBuffer.newBarrier();
        // store consumer
        this.storeProcessor = new BatchEventProcessor<>(this.ringBuffer, barrier, new TickStore(factory, latch));
        // vwap compute consumer
        this.vwapProcessor = new BatchEventProcessor<>(this.ringBuffer, barrier, new TickAvgPrice(instrumentCount, printPrices));
        // prevent buffer wrap before ticks have being handled.
        this.ringBuffer.addGatingSequences(storeProcessor.getSequence(), vwapProcessor.getSequence());
    }

    public void execute() throws InterruptedException {
        this.service.submit(storeProcessor);
        this.service.submit(vwapProcessor);
        for (int i = 0; i < marketCount; i++) {
            this.service.submit(new TickMarket(ringBuffer, instrumentCount, messageCount / marketCount));
        }
        latch.await();
        storeProcessor.halt();
        vwapProcessor.halt();
        service.shutdown();
    }

    public static void main(String[] args) throws JournalException, InterruptedException {
        File dir;
        if (args.length > 0) {
            dir = new File(args[0]);
        } else {
            dir = new File(STORE_DIR);
        }

        if (!dir.exists()) {
            System.out.println("ERROR: Directory does not exist: " + dir.getAbsolutePath());
            System.out.print("\nUsage: " + MultiMarkerTickPersistenceMain.class.getName() + " <storage-directory>\n");
            System.exit(44);
        }

        MultiMarkerTickPersistenceMain main = new MultiMarkerTickPersistenceMain(new JournalFactory(new JournalConfiguration("/tiq.xml", new File(STORE_DIR)).build())
                , 100 // instrument count
                , 2 // market count
                , 200000000 // total number of ticks to publish
                , false // print prices on/off. To print prices please reduce number of ticks to avoid overwhelming console.
        );
        long t = System.currentTimeMillis();
        main.execute();
        System.out.println(System.currentTimeMillis() - t + "ms");
    }
}
