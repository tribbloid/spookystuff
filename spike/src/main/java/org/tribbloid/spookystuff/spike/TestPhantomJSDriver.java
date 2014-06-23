package org.tribbloid.spookystuff.spike;

import org.openqa.selenium.By;
import org.openqa.selenium.Capabilities;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import static com.thoughtworks.selenium.SeleneseTestBase.assertTrue;

/**
 * Created by peng on 05/06/14.
 */
public class TestPhantomJSDriver {
    public static void main(String[] argv) {
        // prepare capabilities
        DesiredCapabilities caps = new DesiredCapabilities();
        caps.setJavascriptEnabled(true);                //< not really needed: JS enabled by default
        caps.setCapability("takesScreenshot", true);    //< yeah, GhostDriver haz screenshotz!
        caps.setCapability(
                PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY,
                "/usr/lib/phantomjs/bin/phantomjs"
        );

        // Launch driver (will take care and ownership of the phantomjs process)
        WebDriver driver = new PhantomJSDriver(caps);

        // Load Google.com
        driver.get("http://www.google.com");
        // Locate the Search field on the Google page
        WebElement element = driver.findElement(By.name("q"));
        // Type Cheese
        String strToSearchFor = "Cheese!";
        element.sendKeys(strToSearchFor);
        // Submit form
        element.submit();

        // Check results contains the term we searched for
        assertTrue(driver.getTitle().toLowerCase().contains(strToSearchFor.toLowerCase()));

        // done
        driver.quit();
    }
}
