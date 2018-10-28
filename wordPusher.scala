import scala.io.Source
import scala.util.Random
import java.net.ServerSocket;
import java.io.BufferedInputStream;
import java.io.PrintStream;
import java.io.BufferedOutputStream;

object wordPusher{
    def main(args: Array[String]) {
        val buffer= scala.io.Source.fromFile("/home/kiran/Pictures/mle_task 2/mle_task/test.csv")
        val lines= (for (line <- buffer.getLines()) yield line).toList//reading lines into list
	val r= new scala.util.Random//created a  random object		
	val ss=new ServerSocket(9999)
	val sock=ss.accept()
	val is=new BufferedInputStream(sock.getInputStream())
	val os=new PrintStream(new BufferedOutputStream(sock.getOutputStream()))	
	while(true)
	{ //infinite generation of words	
	print(lines.apply(r.nextInt(lines.length))) //generating objects within the len of list
	os.println(lines.apply(r.nextInt(lines.length)));//printing to port 9999
	os.flush()		
	}	


    }
}
