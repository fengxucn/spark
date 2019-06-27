package com.fx.demo.utils
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpOptions, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
object Token {

  val tenantId = ""
  val clientId = ""
  val resource = "https%3A%2F%2Fhost"
  val client_secret = ""
  //https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app


  def Token(refresh_token : String) : AzureToken ={
    val params = "client_id=" + clientId + "&refresh_token=" + refresh_token + "&grant_type=refresh_token&resource="+resource+"&client_secret="+client_secret
    val url = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"

    val post = new HttpPost(url)
    post.addHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded")
    post.addHeader(HttpHeaders.ACCEPT_CHARSET, "UTF-8")

    post.setConfig(RequestConfig.custom.setConnectTimeout(60000).setConnectionRequestTimeout(60000).build)

    post.setEntity(new StringEntity(params))

    val resp = HttpClients.createDefault().execute(post)

    try {
      val entity = resp.getEntity
      println(resp.getStatusLine.getStatusCode, resp.getStatusLine.getReasonPhrase)
      implicit val formats = DefaultFormats
      parse(IOUtils.toString(entity.getContent, StandardCharsets.UTF_8)).extract[AzureToken]
    } finally {
      resp.close()
    }
  }
}

case class AzureToken(token_type : String, access_token : String, refresh_token : String, expires_on : String){


  def getToken() : String = {
    token_type + " " + access_token
  }

  def isValid() : Boolean = {
    var status = true
    if(access_token == null)
      status = false
    else{
      val time = DateTime.now().getMillis/1000-5
      if (time >= expires_on.toLong)
        status = false
    }
    status
  }


}

