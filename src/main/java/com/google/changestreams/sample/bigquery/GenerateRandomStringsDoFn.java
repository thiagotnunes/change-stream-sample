package com.google.changestreams.sample.bigquery;

import java.lang.Thread;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.MonotonicallyIncreasing;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UnboundedPerElement
class GenerateRandomStringsDoFn<K, V> extends DoFn<String, Long> {
  private static final Logger LOG = LoggerFactory.getLogger(GenerateRandomStringsDoFn.class);
 
  GenerateRandomStringsDoFn() {
  }
 
  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element String inputElement) {
    return new OffsetRange(0, Long.MAX_VALUE-10000);
  }
 
  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element String inputElement, @Restriction OffsetRange restriction) {
    return new OffsetRangeTracker(new OffsetRange(restriction.getFrom(), restriction.getTo()));
  }
 
  @ProcessElement
  public ProcessContinuation processElement(
      @Element String inputElement,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator watermarkEstimator,
      OutputReceiver<Long> receiver) {
    
    long i = tracker.currentRestriction().getFrom();
    long j = 0;
    long size = 1_000L;
 
    while (j < size) {
      if (!tracker.tryClaim(i+j)) {
        return ProcessContinuation.stop();
      }
      j++;
      com.google.cloud.Timestamp now = com.google.cloud.Timestamp.now();
      receiver.output(now.getSeconds());
 
      try {
        Thread.sleep(10);
      } catch(InterruptedException ex) {
        System.out.println("error:" + ex.getMessage());
        Thread.currentThread().interrupt();
      }
    }
 
    return ProcessContinuation.resume();
  }
 
  @Setup
  public void setup() throws Exception {
    System.out.println("setup is running");
  }
 
  @Teardown
  public void teardown() throws Exception {
    System.out.println("teardown is running");
  }
  
  // The following two watermark functions are important to advance watermark
  // properly.
  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }
 
  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new MonotonicallyIncreasing(watermarkEstimatorState);
  }
}