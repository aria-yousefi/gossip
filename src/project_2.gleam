import argv
import gleam/bool
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor

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
          actors: subjects,
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

// This is a type for each actor's neighbor
pub type Peer { 
  Peer(Int, process.Subject(Message))
}

pub type Message {
  Gossip(String, String, Int)
  SetupPeers(List(Peer))
  ActorCompleted(Int)
  CoordinatorReady(process.Subject(Message))
  AllDone
  PushSum(Float, Float)
}

// As per project spec, these are the 2 algorithms which will be used by main() after parsing input
pub type Algorithm {
  GossipAlgorithm
  PushSumAlgorithm
}

// Common state variables for both gossip and push-sum actors
type ActorState {
  ActorState(
    id: Int,
    times_heard: Int,
    peers: List(Peer),
    coordinator: option.Option(process.Subject(Message)),
    algorithm: Algorithm,
    s: Float,
    w: Float,
    last_ratios: List(Float),
    converged: Bool,
    has_forwarded: Bool,
  )
}

// Coordinator to do 2 things: 1) Seed a rumor, 2) Wait for convergence of all actors in each algorithm
type CoordinatorState {
  CoordinatorState(
    total_actors: Int,
    completed_actors: List(Int),
    main_subject: process.Subject(Message),
    actors: List(process.Subject(Message)),
  )
}

// Coordinator handler waits for AllDone message to exit main program. This wait continues until number of 'ActorCompleted' messages received is equal to the number of actors (shows ActorCompleted is sent when actor is converged)  
fn coordinator_handler(
  state: CoordinatorState,
  message: Message,
) -> actor.Next(CoordinatorState, Message) {
  case message {
    ActorCompleted(actor_id) -> {
      let CoordinatorState(total, completed, main, actors) = state
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
          process.send(main, AllDone)
          let _ = list.each(actors, fn(actor_subject) { process.send(actor_subject, AllDone) })
          actor.stop()
        }
        False -> {
          let new_state = CoordinatorState(total, new_completed, main, actors)
          actor.continue(new_state)
        }
      }
    }

    _ -> actor.continue(state)
  }
}


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
      case state.converged {
        True -> {
          // Passive mode: forward to random neighbor
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
                    <> " (passive) forwarding "
                    <> msg_id
                    <> " to neighbor "
                    <> int.to_string(neighbor_id),
                  )
                  process.send(subject, Gossip(msg_id, "", state.id))
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
          // Active mode
          let was_first = state.times_heard == 0 && !state.has_forwarded
          let new_has_forwarded = case was_first {
            True -> {
              io.println(
                "Actor "
                <> int.to_string(state.id)
                <> " forwarding "
                <> msg_id
                <> " to all peers (first time)",
              )
              forward_to_peers(state.id, state.peers, msg_id)
              True
            }
            False -> state.has_forwarded
          }
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
          // Always push to a random neighbor to generate duplicates
          case state.peers {
            [] -> {
              io.println("Actor " <> int.to_string(state.id) <> " has no peers!")
            }
            peers -> {
              let neighbor_index = int.random(list.length(peers))
              let selected_peer = get_peer_at_index(peers, neighbor_index)
              case selected_peer {
                Ok(Peer(neighbor_id, subject)) -> {
                  io.println(
                    "Actor "
                    <> int.to_string(state.id)
                    <> " pushing "
                    <> msg_id
                    <> " to neighbor "
                    <> int.to_string(neighbor_id),
                  )
                  process.send(subject, Gossip(msg_id, "", state.id))
                }
                Error(_) -> {
                  io.println("Actor " <> int.to_string(state.id) <> " failed to select neighbor")
                }
              }
            }
          }
          let updated_state = ActorState(..state, times_heard: new_times, has_forwarded: new_has_forwarded)
          case new_times >= 3 {
            True -> {
              io.println(
                "Actor "
                <> int.to_string(state.id)
                <> " heard rumor 3 times, converging.",
              )
              // Notify coordinator of convergence for current actor
              case state.coordinator {
                option.Some(coord_subject) -> {
                  process.send(coord_subject, ActorCompleted(state.id))
                }
                option.None -> Nil
              }
              actor.continue(ActorState(..updated_state, converged: True))
            }
            False -> actor.continue(updated_state)
          }
        }
      }
    }

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

    AllDone -> actor.stop()
    ActorCompleted(_) -> actor.continue(state)
  }
}

// Check if push-sum should terminate based on ratio stability over 3 rounds
fn should_terminate_pushsum(ratios: List(Float)) -> Bool {
  case list.length(ratios) >= 3 {
    True -> {
      case ratios {
        [r1, r2, r3, ..] -> {
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
          coordinator: option.None,
          algorithm: algorithm,
          s: int.to_float(next_id + 1),
          w: 1.0,
          last_ratios: [],
          converged: False,
          has_forwarded: False,
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

type TopoEntry {
  TopoEntry(Int, process.Subject(Message), List(Peer))
}

// These functions are used to build the topology based on project spec

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

fn build_line(nodes: List(Peer)) -> List(TopoEntry) {
  let total = list.length(nodes)

  nodes
  |> list.map(fn(node) {
    let Peer(id, subject) = node
    let left = id - 1
    let right = id + 1

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

fn build_grid3d(nodes: List(Peer), n: Int) -> List(TopoEntry) {
  let l = cube_side_for(n)

  nodes
  |> list.map(fn(node) {
    let Peer(id, subject) = node
    let coords = id_to_coords(id, l)
    let neighbor_ids =
      six_neighbors(coords, l)
      |> list.filter(fn(idx) { idx < n })
    let neighbors = neighbors_by_ids(nodes, neighbor_ids)
    TopoEntry(id, subject, neighbors)
  })
}

fn build_imperfect3d(nodes: List(Peer), n: Int) -> List(TopoEntry) {
  let base = build_grid3d(nodes, n)

  base
  |> list.map(fn(entry) {
    let TopoEntry(id, subject, neighbors) = entry
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

// All below are neighbor assigning helper functions when topology is created

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
      }
  }
}

fn id_to_coords(id: Int, l: Int) -> #(Int, Int, Int) {
  let x = id % l
  let y = { id / l } % l
  let z = id / { l * l }
  #(x, y, z)
}

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
  cube_side_loop(1, n)
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

// Make sure that extra neighbor is not self nor already a neighbor
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


fn wait_for_completion(main_subject: process.Subject(Message)) -> Nil {
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