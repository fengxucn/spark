package com.fx.demo.utils

import java.util.{Date, Properties}

import javax.mail._
import javax.mail.internet._

object Email {

  def triggerEmail(
                    emailProp: EmailProp,
                    subject:  String,
                    content:  String): Unit = {

    var message: Message = null

    message = createMessage(emailProp.smtpHost)
    message.setFrom(new InternetAddress(emailProp.fromEmail))

    val addressArray = buildInternetAddressArray(emailProp.toEmail).asInstanceOf[Array[Address]]
    if ((addressArray != null) && (addressArray.length > 0))
    {
      message.setRecipients(Message.RecipientType.TO, addressArray)
    }

    message.setSentDate(new Date())
    message.setSubject(subject)
    message.setText(content)

    Transport.send(message)

  }

  def buildInternetAddressArray(address: String): Array[InternetAddress] = {
    InternetAddress.parse(address)
  }

  def createMessage(smtpHost: String): Message = {
    val properties = new Properties()
    properties.put("mail.smtp.host", smtpHost)
    val session = Session.getDefaultInstance(properties, null)
    new MimeMessage(session)
  }

  def getEmailProperties(properties: Properties): EmailProp = {
    val smtpHost = properties.getProperty("smtpHost","SMTP-GW1.wal-mart.com")
    val toEmail = properties.getProperty("to","feng.xu@wal-mart.com")
    val fromEmail = properties.getProperty("from","castar@wal-mart.com")
    EmailProp(smtpHost,toEmail,fromEmail)
  }

  case class EmailProp(smtpHost:String,var toEmail:String,fromEmail:String)
}
