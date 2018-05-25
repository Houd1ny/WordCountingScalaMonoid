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


  case class State(endCharLeft: Boolean, count: Int, endCharRight: Boolean)

  def main(args: Array[String]): Unit = {

    val bufferedSource = Source.fromFile("/home/yuriyp/projects/ucu/scala_projects/WordCounting/text.txt")
    val text = bufferedSource.mkString.toVector
    bufferedSource.close

    implicit val threshold = 1000


    val monoid = new Monoid[State] {
      def op(x: State, y: State) = {
        (x, y) match {
          case (State(_, -1, _), State(yl, yc, yr)) => y
          case (State(xl, xc, xr), State(_, -1, _)) => x
          case (State(xl, xc, false), State(false, yc, yr)) => State(xl, xc + yc, yr)
          case (State(xl, xc, false), State(true, yc, yr)) => State(xl, xc + yc, yr)
          case (State(xl, xc, true), State(false, yc, yr)) => State(xl, xc + yc + 1, yr)
          case (State(xl, xc, true), State(true, yc, yr)) => State(xl, xc + yc, yr)
        }
      }

      def zero = State(false, -1, false)
    }

    def convertCharIntoState(letter: Char) = {
      if (letter.isLetterOrDigit)
        State(true, 0, true)
      else
        State(false, 0, false)
    }

    val res = foldMapPar(text, 0, text.length, monoid)(convertCharIntoState)

    println(res)
  }
}