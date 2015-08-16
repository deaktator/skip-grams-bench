package deaktator

import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import deaktator.util.SubSeqIterator

import scala.collection.concurrent.TrieMap
import scala.collection.parallel.mutable.ParArray
import scala.{collection => sc}

object SkipGrams {
  val defaultSplitter = Pattern.compile("""\s+""")
  val processors = Runtime.getRuntime.availableProcessors()

  def partitioningIndices(n: Int, k: Int) = {
    if (n < k) Vector((0, n))
    else {
      val starts = 0 until n by n / k
      starts zip starts.tail :+ n
    }
  }

  // Parallel, mutable
  def skipGrams1(str: String,
                 n: Int,
                 k: Int = 0,
                 sep: String = "_",
                 splitString: String = """\s+"""):
  sc.Map[String, AtomicInteger] = {
    val splitter = if (splitString == defaultSplitter.pattern) defaultSplitter else Pattern.compile(splitString)
    val tokens = splitter.split(str)
    val len = tokens.length
    val ind = (0 to len - n).par
    val m = TrieMap.empty[String, AtomicInteger]
    ind.foreach { i =>
      val endExcl = math.min(len, i + n + k)
      if (endExcl - i >= n) {
        val range = i + 1 until math.min(len, i + n + k)
        val it = SubSeqIterator(range, n - 1)
        while(it.hasNext) {
          val gram = new StringBuilder().append(tokens(i))
          val j = it.next().iterator
          while (j.hasNext) {
            gram.append(sep).append(tokens(j.next()))
          }
          val c = m.getOrElseUpdate(gram.toString(), new AtomicInteger(0))
          c.incrementAndGet()
        }
      }
    }
    m
  }

  // Parallel, mutable
  def skipGrams2(str: String,
                 n: Int,
                 k: Int = 0,
                 sep: String = "_",
                 splitString: String = """\s+"""):
  sc.Map[String, AtomicInteger]= {
    val splitter = if (splitString == defaultSplitter.pattern) defaultSplitter else Pattern.compile(splitString)
    val tokens = splitter.split(str)
    val len = tokens.length
    val m = TrieMap.empty[String, AtomicInteger]
    partitioningIndices(len, processors).par.foreach { case (b, e) =>
      var i = b
      while (i < e) {
        val endExcl = math.min(len, i + n + k)
        if (endExcl - i >= n) {
          val range = i + 1 until math.min(len, i + n + k)
          val it = SubSeqIterator(range, n - 1)
          while(it.hasNext) {
            val gram = new StringBuilder().append(tokens(i))
            val j = it.next().iterator
            while (j.hasNext) {
              gram.append(sep).append(tokens(j.next()))
            }
            val c = m.getOrElseUpdate(gram.toString(), new AtomicInteger(0))
            c.incrementAndGet()
          }
        }
        i += 1
      }
    }
    m
  }

  // Parallel, mutable
  def skipGrams3(str: String,
                 n: Int,
                 k: Int = 0,
                 sep: String = "_",
                 splitString: String = """\s+"""):
  sc.Map[String, AtomicInteger] = {
    val splitter = if (splitString == defaultSplitter.pattern) defaultSplitter
    else Pattern.compile(splitString)
    val tokens = splitter.split(str)
    val len = tokens.length
    val m = TrieMap.empty[String, AtomicInteger]
    val ind = (0 to len - n).par
    ind.foreach { i =>
      val endExcl = math.min(len, i + n + k)
      if (endExcl - i >= n) {
        val range = i + 1 until math.min(len, i + n + k)
        val it = SubSeqIterator(range, n - 1)
        while(it.hasNext) {
          val gram = it.next().foldLeft(new StringBuilder().append(tokens(i)))(
                                       (s, j) => s.append(sep).append(tokens(j)))
          val c = m.getOrElseUpdate(gram.toString(), new AtomicInteger(0))
          c.incrementAndGet()
        }
      }
    }
    m
  }

  // Parallel, mutable
  def bagOfWords1(str: String, splitString: String = """\s+"""):
  sc.Map[String, AtomicInteger] = {
    val splitter = if (splitString == defaultSplitter.pattern) defaultSplitter else Pattern.compile(splitString)
    val m = TrieMap.empty[String, AtomicInteger]
    ParArray.handoff(splitter.split(str)).foreach { token =>
      val c = m.getOrElseUpdate(token, new AtomicInteger(0))
      c.incrementAndGet()
    }
    m
  }

  // Simple, immutable
  def bagOfWords2(str: String, splitString: String = """\s+"""): sc.Map[String, Int] =
    str.split(splitString).groupBy(identity).mapValues(_.length)

  // Simple, mutable
  def bagOfWords3(str: String, splitString: String = """\s+"""): sc.Map[String, Int] = {
    val tokens = str.split(splitString)
    val m = scala.collection.mutable.Map[String, Int]()
    var i = tokens.length - 1
    while (i >= 0) {
      val token = tokens(i)
      m.update(token, m.getOrElse(token, 0) + 1)
      i -= 1
    }
    m
  }
}
