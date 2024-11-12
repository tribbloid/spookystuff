package com.tribbloids.spookystuff.lifespan

import org.scalatest.funspec.AnyFunSpec

import java.lang.ref.Cleaner
import java.util.concurrent.atomic.AtomicInteger

object MinimalCleanerSpike {

  private val cleaner = Cleaner.create

  @volatile private var cleaned = false

  val count = new AtomicInteger(0)

  case class CleanerExample() {

    cleaner.register(this, () => cleaned = true)
  }
}

class MinimalCleanerSpike extends AnyFunSpec {

  import MinimalCleanerSpike.*

  it("..") {

    @volatile var vv = new CleanerExample
    //    static volatile CleanerExample vv = null;//    static volatile CleanerExample vv = null;
    //        new CleanerExample();
    vv = null
    while (!cleaned) {
      //            var waste = new byte[512 * 1024 * 1024];
      System.gc()
//      Thread.sleep(1000)
      System.out.println("Waiting to be cleaned...")
    }
    System.out.println("Cleaned")
  }
}
