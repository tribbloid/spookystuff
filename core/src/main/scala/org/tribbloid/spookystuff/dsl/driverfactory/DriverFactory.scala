/*
Copyright 2007-2010 Selenium committers

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package org.tribbloid.spookystuff.dsl.driverfactory

import org.openqa.selenium.Capabilities
import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.session.CleanWebDriver

//TODO: switch to DriverPool! Tor cannot handle too many connection request.
trait DriverFactory extends Serializable{

  final def newInstance(capabilities: Capabilities, spooky: SpookyContext): CleanWebDriver = {
    val result = _newInstance(capabilities, spooky)

    spooky.metrics.driverInitialized += 1
    result
  }

  def _newInstance(capabilities: Capabilities, spooky: SpookyContext): CleanWebDriver
}