package cn.tedu.kryo

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

class MyKryoRegister extends KryoRegistrator {
  
  def registerClasses(kryo: Kryo): Unit = {
    //--实现Kryo的序列化类的注册
    kryo.register(classOf[Person])
    //kryo.register(classOf[Student])
  }
}