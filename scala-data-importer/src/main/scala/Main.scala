package mydata



import java.io.{BufferedReader, File, FileReader, Reader}
import org.apache.tika.parser.Parser

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.Properties
import javax.mail.{Session, Store}
import javax.mail.internet.MimeMessage
import javax.mail.internet.MimeMultipart
import scala.language.postfixOps



object Main {

  def main(args: Array[String]) = {
    val exportsDir = "../exports"

    val takeoutFile = "../exports/takeout.zip"
    val zip = new java.util.zip.ZipFile(takeoutFile)
    val entries = zip.entries


//    import scala.jdk.CollectionConverters._
//    println(entries.asScala.toList.map(_.getName).filter(_.endsWith("MyActivity.json")).sorted.mkString("\n"))
//
//    val activityFile = zip.getEntry("Takeout/My Activity/Chrome/MyActivity.json")
//    println(new String(zip.getInputStream(activityFile).readAllBytes(), "UTF-8"))

    val mboxFile = zip.getEntry("Takeout/Mail/All mail Including Spam and Trash.mbox")
    val inputStream = zip.getInputStream(mboxFile)

    import org.apache.commons.mail.util.MimeMessageParser

    val parser = new MimeMessageParser(new MimeMessage(Session.getDefaultInstance(new Properties()), inputStream))
    parser.parse()

    val subject = parser.getMimeMessage.getSubject
    val from = parser.getFrom
    val to = parser.getTo
    val cc = parser.getCc
    val bcc = parser.getBcc
    println(parser)

    /*
    Walk all files in exportsDir.
    Find ZIP files and print file names in each of them.
    */
    val files = new java.io.File(exportsDir).listFiles
    files.foreach { file =>
      if (file.getName.endsWith(".zip")) {
        println(f"${file}")
        val zip = new java.util.zip.ZipFile(file)
        val entries = zip.entries
        while (entries.hasMoreElements) {
          val entry = entries.nextElement
          //println(f"${file}: ${entry.getName}")
        }
      }
    }



  }

}
