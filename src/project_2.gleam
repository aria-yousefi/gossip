import argv
import gleam/bool
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/string

// ===== Entry point =====

pub fn main() -> Nil {
  let args = argv.load().arguments
  let default_top = "full"
  let default_n = 20
  let default_algorithm = "gossip"

  let top_str = case args {
    [t, _n, _alg] -> t
    [t, _n] -> t
    [t] -> t
    _ -> default_top
  }

  let n = case args {
    [_t, n_str, _alg] -> {
      case int.parse(n_str) {
        Ok(v) -> v
        Error(_) -> default_n
      }
    }
    [_t, n_str] -> {
      case int.parse(n_str) {
        Ok(v) -> v
        Error(_) -> default_n
      }
    }
    _ -> default_n
  }

  let algorithm_str = case args {
    [_t, _n, alg] -> alg
    _ -> default_algorithm
  }

  let algorithm = case algorithm_str {
    "push-sum" -> PushSumAlgorithm
    _ -> GossipAlgorithm
  }

  io.println(
    "Topology: "
    <> top_str
    <> ", n = "
    <> int.to_string(n)
    <> ", algorithm: "
    <> algorithm_str,
  )

  case initialize_topology(n, top_str, algorithm) {
    Ok(subjects) -> {
      io.println("Topology ready. Seeding algorithm...")

      // Create a subject for the main process to receive completion signal
      let main_subject = process.new_subject()

      // Create coordinator
      let coordinator_state =
        CoordinatorState(
          total_actors: n,
          completed_actors: [],
          main_subject: main_subject,
        )

      let assert Ok(coordinator) =
        actor.new(coordinator_state)
        |> actor.on_message(coordinator_handler)
        |> actor.start

      // Send coordinator reference to all actors
      let _ =
        subjects
        |> list.each(fn(subject) {
          process.send(subject, CoordinatorReady(coordinator.data))
        })

      // Start the algorithm
      case subjects {
        [seed_subject, ..] -> {
          case algorithm {
            GossipAlgorithm -> {
              io.println("Seeding gossip into actor 0")
              let _ =
                process.send(
                  seed_subject,
                  Gossip("rumor-1", "Here's a rumor!", -1),
                )
            }
            PushSumAlgorithm -> {
              io.println("Starting push-sum algorithm with actor 0")
              let _ = process.send(seed_subject, PushSum(1.0, 1.0))
            }
          }

          // Wait for coordinator to signal completion
          wait_for_completion(main_subject)
        }
        [] -> io.println("No seed found!")
      }
    }
    Error(_) -> {
      io.println(
        "Failed to initialize topology!\n\nUsage: gleam run <full|line|grid3d|imperfect3d> <n> <gossip|push-sum>",
      )
    }
  }
}

// ===== Messages & public types =====

pub type Peer {
  Peer(Int, process.Subject(Message))
}

pub type Message {
  Gossip(String, String, Int)
  SetupPeers(List(Peer))
  ActorCompleted(Int)
  CoordinatorReady(process.Subject(Message))
  // message to inform all actors have completed execution, I did this to avoid using process.sleep() or similar manual mechanisms to let program flow continue until execution completes
  AllDone
  PushSum(Float, Float)
  // Push-sum message with (s, w) values
  // StartPushSum
  // Signal to start push-sum algorithm
}

// ===== Algorithm types =====

pub type Algorithm {
  GossipAlgorithm
  PushSumAlgorithm
}

// ===== Internal actor state =====

type ActorState {
  ActorState(
    id: Int,
    times_heard: Int,
    peers: List(Peer),
    seen_ids: List(String),
    coordinator: option.Option(process.Subject(Message)),
    algorithm: Algorithm,
    s: Float,
    w: Float,
    last_ratios: List(Float),
    converged: Bool,
  )
}

// ===== Coordinator state =====

type CoordinatorState {
  CoordinatorState(
    total_actors: Int,
    completed_actors: List(Int),
    main_subject: process.Subject(Message),
    // Reference to main process
  )
}

// ===== Coordinator handler =====

fn coordinator_handler(
  state: CoordinatorState,
  message: Message,
) -> actor.Next(CoordinatorState, Message) {
  case message {
    ActorCompleted(actor_id) -> {
      let CoordinatorState(total, completed, main) = state
      let new_completed = [actor_id, ..completed]
      let completed_count = list.length(new_completed)

      io.println(
        "Actor "
        <> int.to_string(actor_id)
        <> " completed. Progress: "
        <> int.to_string(completed_count)
        <> "/"
        <> int.to_string(total),
      )

      case completed_count >= total {
        True -> {
          io.println(
            "All actors have completed! Gossip protocol/Push-sum finished.",
          )
          // Notify main process that we're done
          process.send(main, AllDone)
          actor.stop()
        }
        False -> {
          let new_state = CoordinatorState(total, new_completed, main)
          actor.continue(new_state)
        }
      }
    }

    _ -> actor.continue(state)
  }
}

// ===== Actor handler =====

fn actor_handler(
  state: ActorState,
  message: Message,
) -> actor.Next(ActorState, Message) {
  case message {
    CoordinatorReady(coord_subject) -> {
      let new_state =
        ActorState(..state, coordinator: option.Some(coord_subject))
      actor.continue(new_state)
    }

    SetupPeers(peers) -> {
      io.println(
        "Actor "
        <> int.to_string(state.id)
        <> " received peers list ("
        <> int.to_string(list.length(peers))
        <> " neighbors)",
      )
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
              // Notify coordinator of completion
              case state.coordinator {
                option.Some(coord_subject) -> {
                  process.send(coord_subject, ActorCompleted(state.id))
                }
                option.None -> Nil
              }
              actor.stop()
            }
            False -> actor.continue(updated)
          }
        }
      }
    }

    ActorCompleted(_) -> actor.continue(state)
    // Ignore completion messages in actors
    AllDone -> actor.continue(state)

    PushSum(received_s, received_w) -> {
  case state.converged {
    True -> {
      // Passive mode: forward entire received to random neighbor
      case state.peers {
        [] -> {
          io.println("Actor " <> int.to_string(state.id) <> " (passive) has no peers to forward to!")
          actor.continue(state)
        }
        peers -> {
          let neighbor_index = int.random(list.length(peers))
          let selected_peer = get_peer_at_index(peers, neighbor_index)
          case selected_peer {
            Ok(Peer(neighbor_id, subject)) -> {
              io.println(
                "Actor "
                <> int.to_string(state.id)
                <> " (passive) forwarding ("
                <> float.to_string(received_s)
                <> ", "
                <> float.to_string(received_w)
                <> ") to neighbor "
                <> int.to_string(neighbor_id),
              )
              process.send(subject, PushSum(received_s, received_w))
            }
            Error(_) -> {
              io.println("Actor " <> int.to_string(state.id) <> " (passive) failed to select neighbor")
            }
          }
          actor.continue(state)
        }
      }
    }
    False -> {
      // Active mode: add received, update, check termination
      let new_s = state.s +. received_s
      let new_w = state.w +. received_w
      let current_ratio = new_s /. new_w

      let new_ratios = case list.length(state.last_ratios) >= 3 {
        True -> list.drop(state.last_ratios, 1) |> list.append([current_ratio])
        False -> list.append(state.last_ratios, [current_ratio])
      }

      let updated_state = ActorState(..state, s: new_s, w: new_w, last_ratios: new_ratios)

      case should_terminate_pushsum(new_ratios) {
        True -> {
          io.println(
            "Actor "
            <> int.to_string(state.id)
            <> " converging push-sum. Final ratio: "
            <> float.to_string(current_ratio),
          )
          // Notify coordinator
          case state.coordinator {
            option.Some(coord_subject) -> process.send(coord_subject, ActorCompleted(state.id))
            option.None -> Nil
          }
          // Offload full current mass to random neighbor
          case state.peers {
            [] -> {
              io.println("Actor " <> int.to_string(state.id) <> " has no peers to offload to!")
              actor.continue(ActorState(..updated_state, converged: True, s: 0.0, w: 0.0, last_ratios: []))
            }
            peers -> {
              let neighbor_index = int.random(list.length(peers))
              let selected_peer = get_peer_at_index(peers, neighbor_index)
              case selected_peer {
                Ok(Peer(neighbor_id, subject)) -> {
                  io.println(
                    "Actor "
                    <> int.to_string(state.id)
                    <> " offloading full ("
                    <> float.to_string(new_s)
                    <> ", "
                    <> float.to_string(new_w)
                    <> ") to neighbor "
                    <> int.to_string(neighbor_id),
                  )
                  process.send(subject, PushSum(new_s, new_w))
                }
                Error(_) -> {
                  io.println("Actor " <> int.to_string(state.id) <> " failed to select neighbor for offload")
                }
              }
              // Become passive with zero mass
              actor.continue(ActorState(..updated_state, converged: True, s: 0.0, w: 0.0, last_ratios: []))
            }
          }
        }
        False -> {
          // Normal: send half, keep half
          let half_s = new_s /. 2.0
          let half_w = new_w /. 2.0
          let final_s = new_s -. half_s
          let final_w = new_w -. half_w

          let final_state = ActorState(..updated_state, s: final_s, w: final_w)

          case state.peers {
            [] -> {
              io.println("Actor " <> int.to_string(state.id) <> " has no peers!")
              actor.continue(final_state)
            }
            peers -> {
              let neighbor_index = int.random(list.length(peers))
              let selected_peer = get_peer_at_index(peers, neighbor_index)
              case selected_peer {
                Ok(Peer(neighbor_id, subject)) -> {
                  io.println(
                    "Actor "
                    <> int.to_string(state.id)
                    <> " sending ("
                    <> float.to_string(half_s)
                    <> ", "
                    <> float.to_string(half_w)
                    <> ") to neighbor "
                    <> int.to_string(neighbor_id),
                  )
                  process.send(subject, PushSum(half_s, half_w))
                }
                Error(_) -> {
                  io.println("Actor " <> int.to_string(state.id) <> " failed to select neighbor")
                }
              }
              actor.continue(final_state)
            }
          }
        }
      }
    }
  }
}
  }
}

// ===== Helper functions =====

// Simple list membership check
fn contains(list: List(String), value: String) -> Bool {
  case list {
    [] -> False
    [head, ..tail] -> head == value || contains(tail, value)
  }
}

// Check if push-sum should terminate based on ratio stability
fn should_terminate_pushsum(ratios: List(Float)) -> Bool {
  case list.length(ratios) >= 3 {
    True -> {
      case ratios {
        [r1, r2, r3, ..] -> {
          // Check if the ratio hasn't changed by more than 10^-10 over 3 consecutive rounds
          let diff1 = float.absolute_value(r1 -. r2)
          let diff2 = float.absolute_value(r2 -. r3)
          let should_terminate = diff1 <. 0.0000000001 && diff2 <. 0.0000000001

          io.println(
            "Termination check: r1="
            <> float.to_string(r1)
            <> ", r2="
            <> float.to_string(r2)
            <> ", r3="
            <> float.to_string(r3)
            <> ", diff1="
            <> float.to_string(diff1)
            <> ", diff2="
            <> float.to_string(diff2)
            <> ", terminate="
            <> bool.to_string(should_terminate),
          )

          should_terminate
        }
        _ -> False
      }
    }
    False -> False
  }
}

fn get_peer_at_index(peers: List(Peer), index: Int) -> Result(Peer, Nil) {
  case peers {
    [] -> Error(Nil)
    [peer, ..rest] -> {
      case index {
        0 -> Ok(peer)
        _ -> get_peer_at_index(rest, index - 1)
      }
    }
  }
}

// Forward to all peers except self
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

// ===== Topology creation (now supports multiple topologies) =====

pub fn initialize_topology(
  n: Int,
  topology: String,
  algorithm: Algorithm,
) -> Result(List(process.Subject(Message)), Nil) {
  case n > 0 {
    False -> {
      io.println("No actors to create!")
      Error(Nil)
    }
    True -> {
      let subjects_with_ids = create_actors(n, 0, [], algorithm)

      // Build neighbor lists per actor, depending on the topology
      let neighbor_lists = case topology {
        "full" -> build_full(subjects_with_ids)
        "line" -> build_line(subjects_with_ids)
        "grid3d" -> build_grid3d(subjects_with_ids, n)
        "imperfect3d" -> build_imperfect3d(subjects_with_ids, n)
        _ -> {
          io.println(
            "Unknown topology \"" <> topology <> "\"; defaulting to full.",
          )
          build_full(subjects_with_ids)
        }
      }

      // Send neighbors to each actor
      let _ =
        neighbor_lists
        |> list.each(fn(entry) {
          // entry: (id, subject, neighbors)
          let TopoEntry(id, subject, neighbors) = entry
          io.println(
            "Assigning "
            <> int.to_string(list.length(neighbors))
            <> " neighbors to actor "
            <> int.to_string(id),
          )
          process.send(subject, SetupPeers(neighbors))
        })

      // Return subjects list for seeding
      let subjects =
        subjects_with_ids
        |> list.map(fn(peer) {
          let Peer(_, s) = peer
          s
        })

      Ok(subjects)
    }
  }
}

// Spawn actors and return Peer(id, subject)
fn create_actors(
  remaining: Int,
  next_id: Int,
  acc: List(Peer),
  algorithm: Algorithm,
) -> List(Peer) {
  case remaining > 0 {
    False -> list.reverse(acc)
    True -> {
      let init_state =
        ActorState(
          id: next_id,
          times_heard: 0,
          peers: [],
          seen_ids: [],
          coordinator: option.None,
          algorithm: algorithm,
          s: int.to_float(next_id + 1),  // Optional: start IDs from 1 to n for standard sum/avg; avoids s=0.0 for actor 0
          w: 1.0,
          last_ratios: [],
          converged: False,  // Initialize
        )

      let assert Ok(started) =
        actor.new(init_state)
        |> actor.on_message(actor_handler)
        |> actor.start

      io.println("Actor " <> int.to_string(next_id) <> " created")

      let peer = Peer(next_id, started.data)
      create_actors(remaining - 1, next_id + 1, [peer, ..acc], algorithm)
    }
  }
}

// A compact struct to carry (id, subject, neighbors) while configuring topology
type TopoEntry {
  TopoEntry(Int, process.Subject(Message), List(Peer))
}

// ===== Topology builders =====

// FULL: everyone else is a neighbor
fn build_full(nodes: List(Peer)) -> List(TopoEntry) {
  nodes
  |> list.map(fn(node) {
    let Peer(id, subject) = node
    let neighbors =
      nodes
      |> list.filter(fn(p) {
        let Peer(other_id, _) = p
        other_id != id
      })
    TopoEntry(id, subject, neighbors)
  })
}

// LINE: i has neighbors i-1 and i+1 (bounds-checked)
fn build_line(nodes: List(Peer)) -> List(TopoEntry) {
  let total = list.length(nodes)

  nodes
  |> list.map(fn(node) {
    let Peer(id, subject) = node
    let left = id - 1
    let right = id + 1

    // Multiple subjects in case expression
    let neighbors_ids = case left >= 0, right < total {
      True, True -> [left, right]
      True, False -> [left]
      False, True -> [right]
      False, False -> []
    }

    let neighbors = neighbors_by_ids(nodes, neighbors_ids)
    TopoEntry(id, subject, neighbors)
  })
}

// GRID3D: 6-neighborhood in a LxLxL grid; we only use first n cells
fn build_grid3d(nodes: List(Peer), n: Int) -> List(TopoEntry) {
  let l = cube_side_for(n)

  // Map id -> (x,y,z) within L^3
  nodes
  |> list.map(fn(node) {
    let Peer(id, subject) = node
    let coords = id_to_coords(id, l)
    let neighbor_ids =
      six_neighbors(coords, l)
      |> list.filter(fn(idx) { idx < n })
    // ignore cells beyond n
    let neighbors = neighbors_by_ids(nodes, neighbor_ids)
    TopoEntry(id, subject, neighbors)
  })
}

// IMPERFECT3D: grid3d + 1 extra deterministic neighbor not already present
fn build_imperfect3d(nodes: List(Peer), n: Int) -> List(TopoEntry) {
  let base = build_grid3d(nodes, n)

  base
  |> list.map(fn(entry) {
    let TopoEntry(id, subject, neighbors) = entry
    // deterministic pseudo-random pick: (a * id + b) mod n
    let pick0 = { id * 1_103_515_245 + 12_345 } |> abs_mod(n)
    let pick1 = { pick0 + 1 } |> abs_mod(n)
    let pick = pick_non_conflicting(id, [pick0, pick1], neighbors)

    let extra = case pick {
      Ok(pid) -> {
        case peer_by_id(nodes, pid) {
          Ok(p) -> [p]
          Error(_) -> []
        }
      }
      Error(_) -> []
    }

    let neighbors2 = list.append(neighbors, extra)
    TopoEntry(id, subject, neighbors2)
  })
}

// ===== Neighbor math helpers =====

// Find a peer by id in a small list
fn peer_by_id(nodes: List(Peer), id: Int) -> Result(Peer, Nil) {
  case nodes {
    [] -> Error(Nil)
    [head, ..tail] -> {
      let Peer(hid, _) = head
      case hid == id {
        True -> Ok(head)
        False -> peer_by_id(tail, id)
      }
    }
  }
}

fn neighbors_by_ids(nodes: List(Peer), ids: List(Int)) -> List(Peer) {
  case ids {
    [] -> []
    [i, ..rest] ->
      case peer_by_id(nodes, i) {
        Ok(p) -> [p, ..neighbors_by_ids(nodes, rest)]
        Error(_) -> neighbors_by_ids(nodes, rest)
        // skip invalid
      }
  }
}

// Convert id -> (x,y,z) for side L
fn id_to_coords(id: Int, l: Int) -> #(Int, Int, Int) {
  let x = id % l
  let y = { id / l } % l
  let z = id / { l * l }
  #(x, y, z)
}

// Convert (x,y,z) -> id
fn coords_to_id(x: Int, y: Int, z: Int, l: Int) -> Int {
  x + y * l + z * l * l
}

// Return the six neighbor indices inside the LxLxL box (no n limit here)
fn six_neighbors(coords: #(Int, Int, Int), l: Int) -> List(Int) {
  let #(x, y, z) = coords

  let candidates = [
    #(x - 1, y, z),
    #(x + 1, y, z),
    #(x, y - 1, z),
    #(x, y + 1, z),
    #(x, y, z - 1),
    #(x, y, z + 1),
  ]

  candidates
  |> list.filter(fn(p) {
    let #(cx, cy, cz) = p
    cx >= 0 && cy >= 0 && cz >= 0 && cx < l && cy < l && cz < l
  })
  |> list.map(fn(p) {
    let #(cx, cy, cz) = p
    coords_to_id(cx, cy, cz, l)
  })
}

// Choose L such that L^3 >= n and (L-1)^3 < n
fn cube_side_for(n: Int) -> Int {
  let mut = cube_side_loop(1, n)
  mut
}

fn cube_side_loop(l: Int, n: Int) -> Int {
  case l * l * l >= n {
    True -> l
    False -> cube_side_loop(l + 1, n)
  }
}

fn abs_mod(x: Int, m: Int) -> Int {
  let r = x % m
  case r < 0 {
    True -> r + m
    False -> r
  }
}

// Ensure extra neighbor is not self nor already a neighbor; try candidates in order
fn pick_non_conflicting(
  self_id: Int,
  candidates: List(Int),
  neighbors: List(Peer),
) -> Result(Int, Nil) {
  case candidates {
    [] -> Error(Nil)
    [cand, ..rest] -> {
      case cand == self_id || peer_id_in_list(neighbors, cand) {
        True -> pick_non_conflicting(self_id, rest, neighbors)
        False -> Ok(cand)
      }
    }
  }
}

fn peer_id_in_list(neighbors: List(Peer), target_id: Int) -> Bool {
  case neighbors {
    [] -> False
    [Peer(hid, _), ..tail] ->
      hid == target_id || peer_id_in_list(tail, target_id)
  }
}

// ===== Wait for completion =====

fn wait_for_completion(main_subject: process.Subject(Message)) -> Nil {
  // This will block until the coordinator sends us a message
  let message = process.receive_forever(main_subject)
  case message {
    AllDone -> {
      io.println("Received completion signal from coordinator.")
      Nil
    }
    _ -> {
      io.println("Unexpected message received.")
      Nil
    }
  }
}
