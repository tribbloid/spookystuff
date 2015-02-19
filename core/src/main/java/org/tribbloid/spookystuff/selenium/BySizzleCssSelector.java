package org.tribbloid.spookystuff.selenium;

import org.apache.commons.io.IOUtils;
import org.openqa.selenium.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static java.lang.Thread.currentThread;

//require support from Java
public class BySizzleCssSelector extends By implements Serializable {

  private static final long serialVersionUID = -584931842702178943L;

  private final String selector;

  private static String sizzleSource = null;

  public BySizzleCssSelector(String selector) {
    this.selector = selector;
  }

  @Override
  public WebElement findElement(SearchContext context) {

    List<WebElement> webElements = evaluateSizzleSelector(context, selector);
    return webElements.isEmpty() ? null : webElements.get(0);
  }

  @Override
  public List<WebElement> findElements(SearchContext context) {

    return evaluateSizzleSelector(context, selector);
  }

  @Override
  public String toString() {
    return "By.selector: " + selector;
  }

  protected List<WebElement> evaluateSizzleSelector(SearchContext context, String selector) {
    injectSizzleIfNeeded(context);

    String sizzleSelector = selector.replace("By.selector: ", "");
    if (context instanceof WebElement)
      return executeJavaScript(context, "return Sizzle(arguments[0], arguments[1])", sizzleSelector, context);
    else
      return executeJavaScript(context, "return Sizzle(arguments[0])", sizzleSelector);
  }

  protected void injectSizzleIfNeeded(SearchContext context) {
    if (!sizzleLoaded(context)) {
      injectSizzle(context);
    }
  }

  protected Boolean sizzleLoaded(SearchContext context) {
    try {
      return executeJavaScript(context, "return Sizzle() != null");
    } catch (WebDriverException e) {
      return false;
    }
  }

  protected static synchronized void injectSizzle(SearchContext context) {
    if (sizzleSource == null) {
      try {
        sizzleSource = IOUtils.toString(currentThread().getContextClassLoader().getResource("sizzle.js"));
      } catch (IOException e) {
        throw new RuntimeException("Cannot load sizzle.js from classpath", e);
      }
    }
    executeJavaScript(context, sizzleSource);
  }

  protected static <T> T executeJavaScript(SearchContext context, String jsCode, Object... arguments) {
    return (T) ((JavascriptExecutor) context).executeScript(jsCode, arguments);
  }
}