import scala.io.Source
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveTask
import java.util.concurrent.ForkJoinWorkerThread

object Main {
  val forkJoinPool = new ForkJoinPool

  def task[T](computation: => T): RecursiveTask[T] = {
    val t = new RecursiveTask[T] {
      def compute = computation
    }

    Thread.currentThread match {
      case wt: ForkJoinWorkerThread =>
        t.fork() // schedule for execution
      case _ =>
        forkJoinPool.execute(t)
    }

    t
  }

  def parallel[A, B](taskA: => A, taskB: => B): (A, B) = {
    val right = task { taskB }
    val left = taskA

    (left, right.join())
  }

  def parallel[A, B, C, D](taskA: => A, taskB: => B, taskC: => C, taskD: => D): (A, B, C, D) = {
    val ta = task { taskA }
    val tb = task { taskB }
    val tc = task { taskC }
    val td = taskD
    (ta.join(), tb.join(), tc.join(), td)
  }


  trait Monoid[A] {
    def op(x: A, y: A): A
    def zero: A
  }

  def foldMapPar[A, B](xs: IndexedSeq[A],
                       from: Int, to: Int,
                       m: Monoid[B])
                      (f: A => B)
                      (implicit thresholdSize: Int): B =
    if (to - from <= thresholdSize)
      foldMapSegment(xs, from, to, m)(f)
    else {
      val middle = from + (to - from) / 2
      val (l, r) = parallel(
        foldMapPar(xs, from, middle, m)(f)(thresholdSize),
        foldMapPar(xs, middle, to, m)(f)(thresholdSize))

      m.op(l, r)
    }

  def foldMapSegment[A, B](xs: IndexedSeq[A],
                           from: Int, to: Int,
                           m: Monoid[B])
                          (f: A => B): B = {
    var res = m.zero
    var index = from
    while (index < to) {
      res = m.op(res, f(xs(index)))
      index = index + 1
    }
    res
  }

  def isAlphaNum(c: Char) =
    (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '1')

  case class WordCountTuple(leftWord: Boolean, count: Int, rightWord: Boolean)

  def main(args: Array[String]): Unit = {

    val bufferedSource = Source.fromFile("/home/yuriyp/projects/ucu/scala_projects/WordCounting/text.txt")
    val text = bufferedSource.mkString.toVector
    bufferedSource.close

    implicit val threshold = 1000


    val monoid = new Monoid[WordCountTuple] {
      def op(x: WordCountTuple, y: WordCountTuple) = {
        (x, y) match {
          case (WordCountTuple(_, -1, _), WordCountTuple(yl, yc, yr)) => y
          case (WordCountTuple(xl, xc, xr), WordCountTuple(_, -1, _)) => x
          case (WordCountTuple(xl, xc, false), WordCountTuple(false, yc, yr)) => WordCountTuple(xl, xc + yc, yr)
          case (WordCountTuple(xl, xc, false), WordCountTuple(true, yc, yr)) => WordCountTuple(xl, xc + yc, yr)
          case (WordCountTuple(xl, xc, true), WordCountTuple(false, yc, yr)) => WordCountTuple(xl, xc + yc + 1, yr)
          case (WordCountTuple(xl, xc, true), WordCountTuple(true, yc, yr)) => WordCountTuple(xl, xc + yc, yr)
        }
      }

      def zero = WordCountTuple(false, -1, false)
    }

    def convertCharIntoWordCountTuple(letter: Char) = {
      if (isAlphaNum(letter) )
        WordCountTuple(true, 0, true)
      else
        WordCountTuple(false, 0, false)
    }

    val res = foldMapPar(text, 0, text.length, monoid)(convertCharIntoWordCountTuple)

    println(res)
  }
}