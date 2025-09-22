package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.dsl.BinaryDeployment

// TODO: this class may be useless: both Selenium & Playwright can download browsers automatically
case class BrowserDeployment(
    override val targetPath: String = ???,
    override val repositoryURI: String = ???
) extends BinaryDeployment {}
