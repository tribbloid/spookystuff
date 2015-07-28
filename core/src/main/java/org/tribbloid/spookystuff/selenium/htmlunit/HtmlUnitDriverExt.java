package org.tribbloid.spookystuff.selenium.htmlunit;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import org.openqa.selenium.Capabilities;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;

/**
 * Created by peng on 28/07/15.
 */
public class HtmlUnitDriverExt extends HtmlUnitDriver {

  public HtmlUnitDriverExt(BrowserVersion version) {
    super(version);
  }

  public HtmlUnitDriverExt() {
    super();
  }

  public HtmlUnitDriverExt(boolean enableJavascript) {
    super(enableJavascript);
  }

  /**
   * Note: There are two configuration modes for the HtmlUnitDriver using this constructor. The
   * first is where the browserName is "firefox", "internet explorer" and browserVersion denotes the
   * desired version. The second one is where the browserName is "htmlunit" and the browserVersion
   * denotes the required browser AND its version. In this mode the browserVersion could either be
   * "firefox" for Firefox or "internet explorer-7" for IE 7. The Remote WebDriver uses the second
   * mode - the first mode is deprecated and should not be used.
   */
  public HtmlUnitDriverExt(Capabilities capabilities) {
    super(capabilities);
  }

  public WebClient modifyWebClient(WebClient client) {
//    client.getOptions().setThrowExceptionOnScriptError(false);
    return client;
  }
}
