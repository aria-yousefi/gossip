import gleam/erlang/process
import gleam/io
import gleam/otp/actor

pub fn main() -> Nil {
  let rumor = "Here's a rumor!"
  case initialize_topology(3) {
    Ok(actor.Started(pid: _pid, data: subject)) -> {
      io.println("Starting gossip process...")
      let _ = process.send(subject, Gossip(rumor))
      io.println("Gossip message sent!")
      process.sleep(10)
    }
    Error(_) -> {
      io.print("Failed to initialize topology!")
    }
  }
}

pub type Message {
  Gossip(String)
}

pub fn gossip_handler(state: Int, message: Message) -> actor.Next(Int, Message) {
  case message {
    Gossip(_rumor) -> {
      case state < 3 {
        True -> {
          let updated_state = state + 1
          actor.continue(updated_state)
        }
        False -> {
          io.println("Not receiving any more rumors!")
          actor.continue(state)
        }
      }
    }
  }
}

pub fn initialize_topology(
  n: Int,
) -> Result(actor.Started(process.Subject(Message)), Nil) {
  let topology = "full"
  case topology {
    "full" -> create_full_network(n)
    _ -> {
      io.println("Invalid topology!")
      Error(Nil)
    }
  }
}

pub fn create_full_network(
  n: Int,
) -> Result(actor.Started(process.Subject(Message)), Nil) {
  case n > 0 {
    True -> {
      let assert Ok(first_actor) =
        actor.new(0) |> actor.on_message(gossip_handler) |> actor.start
      io.print("First actor created!")
      let _ = create_remaining_actors(n - 1)
      Ok(first_actor)
    }
    False -> {
      io.println("No actors to create!")
      Error(Nil)
    }
  }
}

pub fn create_remaining_actors(n: Int) -> Nil {
  case n > 0 {
    True -> {
      let assert Ok(_actor) =
        actor.new(0) |> actor.on_message(gossip_handler) |> actor.start
      io.println("Actor created!")
      create_remaining_actors(n - 1)
    }
    False -> {
      io.println("Topology initialized!")
      Nil
    }
  }
}
