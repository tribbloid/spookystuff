//package com.tribbloids.spookystuff.selenium;
//
//import com.gargoylesoftware.htmlunit.BrowserVersion;
//import com.gargoylesoftware.htmlunit.Page;
//import com.gargoylesoftware.htmlunit.WebResponse;
//import org.openqa.selenium.Capabilities;
//import org.openqa.selenium.htmlunit.HtmlUnitDriver;
//
///**
// * Created by peng on 3/10/15.
// */
//public class RationalHtmlUnitDriver extends HtmlUnitDriver {
//
//  public RationalHtmlUnitDriver(BrowserVersion version) {
//    super(version);
//  }
//
//  public RationalHtmlUnitDriver() {
//    this(false);
//  }
//
//  public RationalHtmlUnitDriver(boolean enableJavascript) {
//    super(enableJavascript);
//  }
//
//  /**
//   * Note: There are two configuration modes for the HtmlUnitDriver using this constructor. The
//   * first is where the browserName is "firefox", "internet explorer" and browserVersion denotes the
//   * desired version. The second one is where the browserName is "htmlunit" and the browserVersion
//   * denotes the required browser AND its version. In this mode the browserVersion could either be
//   * "firefox" for Firefox or "internet explorer-7" for IE 7. The Remote WebDriver uses the second
//   * mode - the first mode is deprecated and should not be used.
//   */
//  public RationalHtmlUnitDriver(Capabilities capabilities) {
//    super(capabilities);
//  }
//  @Override
//  public String getPageSource() {
//    Page page = lastPage();
//    if (page == null) {
//      return null;
//    }
//
//    WebResponse response = page.getWebResponse();
//    return response.getContentAsString();
//  }
//}
