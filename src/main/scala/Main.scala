import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}

object CarrefourMain {
  // On utilise tes zones 1, 2, 3, 4
  val zones = Vector("1", "2", "3", "4")

  def apply(): Behavior[Unit] = Behaviors.setup { context =>
    val hub = context.spawn(HubCentral(), "HubCentral")
    
    // On crée 12 voies
    for (i <- 1 to 12) {
      val zoneInitiale = zones((i - 1) % 4)
      context.spawn(CapteurVoie(i, zoneInitiale, hub), s"CapteurVoie_$i")
    }
    Behaviors.ignore
  }

  def main(args: Array[String]): Unit = {
    val system: ActorSystem[Unit] = ActorSystem(CarrefourMain(), "SimulationCarrefour2026")
    
    println("======= 🚦 SIMULATION LANCÉE =======")
    println("Quadrants : 2 (NE) -> 1 (NO) -> 3 (SO) -> 4 (SE)")
    
    try {
      Thread.currentThread().join() 
    } catch {
      case _: InterruptedException => system.terminate()
    }
  }
}