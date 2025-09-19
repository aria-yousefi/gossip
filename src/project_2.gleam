import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor

// ===== Entry point =====

pub fn main() -> Nil {
  let n = 20
  // smaller for easier output; bump to 1000 later
  let rumor_id = "rumor-1"
  let rumor_text = "Here's a rumor!"

  case initialize_full_topology(n) {
    Ok(subjects) -> {
      io.println("Topology ready. Seeding gossip...")
      case subjects {
        [seed_subject, ..] -> {
          io.println("Seeding gossip into actor 0")
          let _ = process.send(seed_subject, Gossip(rumor_id, rumor_text, -1))
          process.sleep(3000)
          // ms to let actors run
        }
        [] -> io.println("No seed found!")
      }
    }
    Error(_) -> io.println("Failed to initialize topology!")
  }
}

// ===== Messages & public types =====

pub type Peer {
  Peer(Int, process.Subject(Message))
}

pub type Message {
  Gossip(String, String, Int)
  SetupPeers(List(Peer))
}

// ===== Internal actor state =====

type ActorState {
  ActorState(
    id: Int,
    times_heard: Int,
    peers: List(Peer),
    seen_ids: List(String),
    // dedupe list
  )
}

// ===== Actor handler =====

fn actor_handler(
  state: ActorState,
  message: Message,
) -> actor.Next(ActorState, Message) {
  case message {
    SetupPeers(peers) -> {
      io.println("Actor " <> int.to_string(state.id) <> " received peers list")
      let new_state = ActorState(..state, peers: peers)
      actor.continue(new_state)
    }

    Gossip(msg_id, _text, sender_id) -> {
      case state.times_heard >= 3 {
        True -> actor.stop()
        False -> {
          let new_times = state.times_heard + 1
          io.println(
            "Actor "
            <> int.to_string(state.id)
            <> " heard rumor "
            <> msg_id
            <> " from "
            <> int.to_string(sender_id)
            <> " ["
            <> int.to_string(new_times)
            <> "/3]",
          )

          let already_forwarded = contains(state.seen_ids, msg_id)
          let new_seen_ids = case already_forwarded {
            True -> state.seen_ids
            False -> [msg_id, ..state.seen_ids]
          }

          let _ = case already_forwarded {
            True -> Nil
            False -> {
              io.println(
                "Actor "
                <> int.to_string(state.id)
                <> " forwarding rumor "
                <> msg_id,
              )
              forward_to_peers(state.id, state.peers, msg_id)
            }
          }

          let updated =
            ActorState(..state, times_heard: new_times, seen_ids: new_seen_ids)

          case new_times >= 3 {
            True -> {
              io.println(
                "Actor "
                <> int.to_string(state.id)
                <> " heard rumor 3 times, stopping.",
              )
              actor.stop()
            }
            False -> actor.continue(updated)
          }
        }
      }
    }
  }
}

// ===== Helper functions =====

fn contains(list: List(String), value: String) -> Bool {
  case list {
    [] -> False
    [head, ..tail] -> head == value || contains(tail, value)
  }
}

fn forward_to_peers(self_id: Int, peers: List(Peer), msg_id: String) -> Nil {
  let _ =
    peers
    |> list.filter(fn(peer) {
      let Peer(id, _) = peer
      id != self_id
    })
    |> list.each(fn(peer) {
      let Peer(_, subject) = peer
      process.send(subject, Gossip(msg_id, "", self_id))
    })
  Nil
}

// ===== Topology creation =====

pub fn initialize_full_topology(
  n: Int,
) -> Result(List(process.Subject(Message)), Nil) {
  case n > 0 {
    False -> {
      io.println("No actors to create!")
      Error(Nil)
    }
    True -> {
      let subjects_with_ids = create_actors(n, 0, [])

      let subjects =
        subjects_with_ids
        |> list.map(fn(peer) {
          let Peer(_, s) = peer
          s
        })

      let _ =
        subjects_with_ids
        |> list.each(fn(peer) {
          let Peer(_, s) = peer
          process.send(s, SetupPeers(subjects_with_ids))
        })

      Ok(subjects)
    }
  }
}

fn create_actors(remaining: Int, next_id: Int, acc: List(Peer)) -> List(Peer) {
  case remaining > 0 {
    False -> list.reverse(acc)
    True -> {
      let init_state =
        ActorState(id: next_id, times_heard: 0, peers: [], seen_ids: [])

      let assert Ok(started) =
        actor.new(init_state)
        |> actor.on_message(actor_handler)
        |> actor.start

      io.println("Actor " <> int.to_string(next_id) <> " created")

      let peer = Peer(next_id, started.data)
      create_actors(remaining - 1, next_id + 1, [peer, ..acc])
    }
  }
}
