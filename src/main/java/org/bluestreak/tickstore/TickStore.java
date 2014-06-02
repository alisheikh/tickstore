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

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.JournalFactory;

import java.util.concurrent.CountDownLatch;

public class TickStore implements EventHandler<Tick>, LifecycleAware {

    private final JournalFactory factory;
    private final CountDownLatch latch;
    private JournalWriter<Tick> writer;

    public TickStore(JournalFactory factory, CountDownLatch latch) {
        this.factory = factory;
        this.latch = latch;
    }

    @Override
    public void onStart() {
        try {
            this.writer = factory.bulkWriter(Tick.class);
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void onShutdown() {
        try {
            System.out.println("Persisted: " + writer.size());
        } catch (JournalException e) {
            System.out.println("Corrupt storage");
        }
        this.writer.close();
    }

    @Override
    public void onEvent(Tick event, long sequence, boolean endOfBatch) throws Exception {
        switch (event.instrument) {
            case -1:
                // market close message, don't persist
                latch.countDown();
                break;
            default:
                // add timestamp and save
                event.timestamp = System.currentTimeMillis();
                writer.append(event);
        }
        // commit at end of batch
        if (endOfBatch) {
            writer.commit();
        }
    }
}
