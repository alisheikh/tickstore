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

public class TickAvgPrice implements EventHandler<Tick> {

    private final long bidVolume[];
    private final long askVolume[];
    private final long bidPrice[];
    private final long askPrice[];
    private final StringBuilder builder;
    private final boolean print;

    public TickAvgPrice(int instrumentCount, boolean print) {
        this.bidVolume = new long[instrumentCount];
        this.askVolume = new long[instrumentCount];
        this.bidPrice = new long[instrumentCount];
        this.askPrice = new long[instrumentCount];
        this.builder = new StringBuilder();
        this.print = print;
    }

    @Override
    public void onEvent(Tick event, long sequence, boolean endOfBatch) throws Exception {
        if (event.instrument != -1) {
            switch (event.bidAsk) {
                case 'b':
                    bidVolume[event.instrument] += event.volume;
                    bidPrice[event.instrument] += (event.price * event.volume);
                    break;
                default:
                    askVolume[event.instrument] += event.volume;
                    askPrice[event.instrument] += (event.price * event.volume);
                    break;
            }

            long bidVwap = bidVolume[event.instrument] == 0 ? 0 : bidPrice[event.instrument] / bidVolume[event.instrument];
            long askVwap = askVolume[event.instrument] == 0 ? 0 : askPrice[event.instrument] / askVolume[event.instrument];

            builder.setLength(0);
            builder.append("i:").append(event.instrument).append(", ");
            builder.append("b:").append(bidVwap).append(", ");
            builder.append("a:").append(askVwap);

            if (print) {
                for (int i = 0; i < builder.length(); i++) {
                    System.out.print(builder.charAt(i));
                }
                System.out.println();
            }
        }
    }
}
