package deaktator

import com.google.caliper.Benchmark
import com.google.caliper.Param
import com.google.caliper.BeforeExperiment
import org.apache.commons.vfs2.VFS

class BagOfWordsBench {
  @Param(Array("32","64","128","256", "512", "1000","10000","100000","1000000")) var size: Int = _

  private[this] val pi1000000 = scala.io.Source.fromInputStream(VFS.getManager.resolveFile("res:pi_1000000.txt").getContent.getInputStream).getLines().mkString

  private[this] var pi: String = _

  private[this] var split: String = _

  @BeforeExperiment def before(): Unit = {
    pi = pi1000000 take size
    split = (math.round(math.log10(size)).toInt + 1) + "+"
  }

  @Benchmark def skipGrams1(reps: Int): Int = {
    var i = 0
    val dummy = 0
    while (i < reps) {
      SkipGrams.skipGrams1(pi, 1, splitString = split)
      i += 1
    }
    dummy
  }

  @Benchmark def skipGrams2(reps: Int): Int = {
    var i = 0
    val dummy = 0
    while (i < reps) {
      SkipGrams.skipGrams2(pi, 1, splitString = split)
      i += 1
    }
    dummy
  }

  @Benchmark def skipGrams3(reps: Int): Int = {
    var i = 0
    val dummy = 0
    while (i < reps) {
      SkipGrams.skipGrams2(pi, 1, splitString = split)
      i += 1
    }
    dummy
  }

  @Benchmark def bagOfWords1(reps: Int): Int = {
    var i = 0
    val dummy = 0
    while (i < reps) {
      SkipGrams.bagOfWords1(pi, splitString = split)
      i += 1
    }
    dummy
  }

  @Benchmark def bagOfWords2(reps: Int): Int = {
    var i = 0
    val dummy = 0
    while (i < reps) {
      SkipGrams.bagOfWords2(pi, splitString = split)
      i += 1
    }
    dummy
  }

  @Benchmark def bagOfWords3(reps: Int): Int = {
    var i = 0
    val dummy = 0
    while (i < reps) {
      SkipGrams.bagOfWords2(pi, splitString = split)
      i += 1
    }
    dummy
  }
}
