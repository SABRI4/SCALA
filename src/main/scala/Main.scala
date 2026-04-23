import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import scala.concurrent.duration._

object CarrefourMain {
  // Collection d'élements pour faciliter la création des capteurs de voie
  val zones = Vector("NE", "NO", "SE", "SO")

  def apply(): Behavior[Unit] = Behaviors.setup { context =>
    val hub = context.spawn(HubCentral(), "HubCentral")
    context.log.info("Système de contrôle du carrefour démarré.")

    for (i <- 1 to 12) {
      val zoneCible = zones((i - 1) % 4)
      context.spawn(CapteurVoie(i, zoneCible, hub), s"CapteurVoie_$i")
    }

    // On ne fait rien, mais on reste vivant
    Behaviors.ignore 
  }

  def main(args: Array[String]): Unit = {

    // On crée le système
    val system: ActorSystem[Unit] = ActorSystem(CarrefourMain(), "SimulationCarrefour2026")
    
    println("======= SIMULATION LANCÉE =======")
    
    try {
      // On attend indéfiniment (ou jusqu'au Ctrl+C)
      Thread.currentThread().join() 
    } catch {
      case _: InterruptedException => 
        system.terminate()
    }
  }
}