package es.weso.pschema
import cats._
import cats.data._
import cats.implicits._
import org.apache.spark.graphx._

sealed abstract class MsgLabel[+L, +E, +P] extends Product with Serializable {

  private def showSet[A](x: Set[A]): String = {
    if (x.isEmpty) "{}"
    else x.map(_.toString).mkString(",")
  }

  private def showNel[A](x: NonEmptyList[A]): String = {
    x.map(_.toString).toList.mkString(",")
  }

  private def showpairNel[A,B](xs: Set[(A, NonEmptyList[B])]): String = 
    showSet(xs.map{ case (dt, es) => s"$dt - ${showNel(es)}"})

  override def toString: String = this match {
    case ValidateLabel => "Validate"
    case Validated(dt) => s"Validated${showSet(dt)}"
    case NotValidated(dtes) => s"NotValidated${showpairNel(dtes)}"
    case InconsistentLabel(okts, failts) => s"Inconsistent(oks=${showSet(okts)}, fails=${showpairNel(failts)}"
    case WaitFor(ds) => s"WaitFor ${showSet(ds)}"
  }
}
case object ValidateLabel extends MsgLabel[Nothing,Nothing,Nothing]
case class Validated[L,P](dt: Set[DependTriple[P,L]]) extends MsgLabel[L, Nothing, P]
case class NotValidated[L,E,P](dtes: Set[(DependTriple[P,L], NonEmptyList[E])]) extends MsgLabel[L, E, P]
case class InconsistentLabel[L,E,P](okts: Set[DependTriple[P,L]], failts: Set[(DependTriple[P,L], NonEmptyList[E])]) extends MsgLabel[L,E,P]
case class WaitFor[L,P](ds: Set[DependTriple[P,L]]) extends MsgLabel[L,Nothing,P]

object MsgLabel {

  implicit def MsgLabelMonoid[L,E,P]: Semigroup[MsgLabel[L,E,P]] = new Semigroup[MsgLabel[L,E,P]] {
    override def combine(x: MsgLabel[L,E,P], y: MsgLabel[L,E,P]): MsgLabel[L,E,P] = x match {
      case ValidateLabel => y 
      case vdx: Validated[L,P] => y match {
        case ValidateLabel => x 
        case vdy: Validated[L,P] => {
          Validated[L,P](vdx.dt union vdy.dt)
        }
        case nvdy: NotValidated[L,E,P] => InconsistentLabel(vdx.dt,nvdy.dtes)
        case incy: InconsistentLabel[L,E,P] => InconsistentLabel(incy.okts union vdx.dt, incy.failts)
        case wfy: WaitFor[L,P] => Validated(vdx.dt union wfy.ds)
      }

      case nvdx: NotValidated[L,E,P] => y match {
        case ValidateLabel => x
        case vdy: Validated[L,P] => InconsistentLabel(vdy.dt,nvdx.dtes)
        case nvdy: NotValidated[L,E,P] => NotValidated(nvdx.dtes union nvdy.dtes)
        case inc: InconsistentLabel[L,E,P] => InconsistentLabel(inc.okts, inc.failts union nvdx.dtes)
        case wf: WaitFor[L,P] => NotValidated(nvdx.dtes) // TODO: do we ignore wf?
      }
      case inc: InconsistentLabel[L,E,P] => y match {
        case ValidateLabel => x 
        case vdy: Validated[L,P] => InconsistentLabel(inc.okts union vdy.dt, inc.failts)
        case nvdy: NotValidated[L, E, P] => InconsistentLabel(inc.okts, inc.failts) // do we ignore nvdy ?
        case incy: InconsistentLabel[L,E,P] => InconsistentLabel(inc.okts union incy.okts, inc.failts union incy.failts)
        case wf: WaitFor[L,P] => InconsistentLabel(inc.okts union wf.ds, inc.failts) // TODO: do we ignode wf?
      }
      case wf: WaitFor[L,P] => y match {
        case ValidateLabel => x
        case vd: Validated[L,P] => Validated(vd.dt union wf.ds)
        case nvd: NotValidated[L,E,P] => NotValidated(nvd.dtes) // TODO: do we ignore wf info?
        case inc: InconsistentLabel[L,E,P] => InconsistentLabel(inc.okts union wf.ds, inc.failts) // TODO: Do we ignore wf info?
        case wfy: WaitFor[L,P] => WaitFor(wf.ds union wfy.ds)
      }
    }
      
  }
}

case class MsgMap[L, E, P: Ordering](
 mmap: Map[L,MsgLabel[L,E,P]]
) extends Serializable {

  def withValidateSingle(lbl: L): MsgMap[L,E,P] = mmap.get(lbl) match {
    case None => MsgMap(mmap.updated(lbl, ValidateLabel))
    case Some(m) => MsgMap(mmap.updated(lbl, m.combine(ValidateLabel)))
  }

  def withValidatedSingle(d: DependInfo[P,L]): MsgMap[L,E,P] = {
   val msgLabel =  Validated(Set(d.dependTriple))
   val lbl = d.srcLabel
   MsgMap(mmap.combine(Map(lbl -> msgLabel)))
  }

  def withNotValidatedSingle(d: DependInfo[P,L], es: NonEmptyList[E]): MsgMap[L,E,P] = {
    MsgMap(mmap.combine(Map(d.srcLabel -> NotValidated(Set((d.dependTriple, es))))))
  }

  def withWaitForSingle(d: DependInfo[P,L]): MsgMap[L,E,P] = {
    MsgMap(mmap.combine(Map(d.srcLabel -> WaitFor(Set(d.dependTriple)))))
  }
    
  def withValidate(v: Set[L]): MsgMap[L, E, P] = v.foldLeft(this){
    case (acc,lbl) => acc.withValidateSingle(lbl)
  }

  def withValidated(v: Set[DependInfo[P,L]]): MsgMap[L, E, P] = v.foldLeft(this) {
    case (acc,di) => acc.withValidatedSingle(di)
  }

  def withNotValidated(v: Set[(DependInfo[P,L],NonEmptyList[E])]): MsgMap[L, E, P] = v.foldLeft(this) {
    case (acc,(di,es)) => acc.withNotValidatedSingle(di,es)
  }

  def withWaitFor(dis: Set[DependInfo[P,L]]): MsgMap[L, E, P] = dis.foldLeft(this) {
    case (acc, di) => acc.withWaitForSingle(di)
  }

  def merge(other: MsgMap[L, E, P]): MsgMap[L, E, P] = MsgMap(mmap.combine(other.mmap))

  override def toString = {
    val sb = new StringBuilder()
    sb.append(s"MsgMap:\n")
    mmap.foldLeft(sb){
      case (acc,(lbl,msgLabel)) => sb.append(s"$lbl -> $msgLabel")
    }
    sb.toString
  }
 } 

object MsgMap {

    def empty[VD,L,E, P: Ordering]: MsgMap[L,E, P] = MsgMap(Map())

    def validate[L, E, P: Ordering](
      shapes: Set[L]
      ): MsgMap[L, E, P] = 
      empty.withValidate(shapes)

    def waitFor[L, E, P:Ordering](srcShape: L, p:P, dstShape: L, dst: VertexId): MsgMap[L, E, P] = 
      empty.withWaitFor(Set(DependInfo(srcShape, DependTriple(dst, p, dstShape))))

    def validated[L, E, P:Ordering](srcShape: L, p: P, dstShape: L, dst: VertexId): MsgMap[L, E, P] =
      empty.withValidated(Set(DependInfo(srcShape,DependTriple(dst,p,dstShape))))

    def notValidated[L, E, P:Ordering](l: L, p: P, lbl: L, v: VertexId, es: NonEmptyList[E]): MsgMap[L,E,P] =
      empty.withNotValidated(Set((DependInfo(l, DependTriple(v, p,lbl)), es)))

  }
