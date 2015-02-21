package org.tribbloid.spookystuff.selenium;

import org.apache.commons.io.IOUtils;
import org.openqa.selenium.*;

import java.io.Serializable;
import java.util.List;

public class BySizzleCssSelector extends By implements Serializable {

  private static final long serialVersionUID = -584931842702178943L;

  private final String selector;

  private static String sizzleSource = null;

  public BySizzleCssSelector(String selector) {
    this.selector = selector;
  }

  @Override
  public List<WebElement> findElements(SearchContext context) {

    if (context instanceof JavascriptExecutor) {
      return evaluateSizzleSelector((JavascriptExecutor) context, selector);
    }
    else {
      throw new WebDriverException(
        "Driver does not support finding an element by selector: " + selector);
    }
  }

  @Override
  public String toString() {
    return "By.selector: " + selector;
  }

  protected List<WebElement> evaluateSizzleSelector(JavascriptExecutor context, String selector) {
    injectSizzleIfNeeded(context);

    String sizzleSelector = selector.replace("By.selector: ", "");
    if (context instanceof WebElement)
      return executeJavaScript(context, "return Sizzle(arguments[0], arguments[1])", sizzleSelector, context);
    else
      return executeJavaScript(context, "return Sizzle(arguments[0])", sizzleSelector);
  }

  protected void injectSizzleIfNeeded(JavascriptExecutor context) {
    if (!sizzleLoaded(context)) {
      injectSizzle(context);
    }
  }

  protected Boolean sizzleLoaded(JavascriptExecutor context) {
    try {
      return executeJavaScript(context, "return Sizzle() != null");
    } catch (WebDriverException e) {
      return false;
    }
  }

  protected synchronized void injectSizzle(JavascriptExecutor context) {
    if (sizzleSource == null) {
      try {
        sizzleSource = IOUtils.toString(this.getClass().getResource("/sizzle.js"));
      } catch (Throwable e) {
        throw new RuntimeException("Cannot load sizzle.js from classpath", e);
      }
    }
    executeJavaScript(context, sizzleSource);
  }

  protected static <T> T executeJavaScript(JavascriptExecutor context, String jsCode, Object... arguments) {
    return (T) context.executeScript(jsCode, arguments);
  }
}