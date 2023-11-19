/**
 * Precompiled [ai.acyclic.scala-mixin.gradle.kts][Ai_acyclic_scala_mixin_gradle] script plugin.
 *
 * @see Ai_acyclic_scala_mixin_gradle
 */
public
class Ai_acyclic_scalaMixinPlugin : org.gradle.api.Plugin<org.gradle.api.Project> {
    override fun apply(target: org.gradle.api.Project) {
        try {
            Class
                .forName("Ai_acyclic_scala_mixin_gradle")
                .getDeclaredConstructor(org.gradle.api.Project::class.java, org.gradle.api.Project::class.java)
                .newInstance(target, target)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }
}
