package cn.tarena.ssort

class Ssort(val v1:String, val v2:Int) 
               extends Ordered[Ssort] with Serializable {
  
  def compare(that: Ssort): Int = {
      //--先按第一列做升序比较
      val result=this.v1.compareTo(that.v1)
      if(result==0){
        //--按第二列做降序排序
        that.v2.compareTo(this.v2)
      }else{
        result
      }
  }
}