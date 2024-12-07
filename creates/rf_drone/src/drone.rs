use crossbeam_channel::{select_biased, Receiver, Sender};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use rand::{rng, Rng};

use wg_2024::controller::{DroneCommand, DroneEvent};
use wg_2024::drone::Drone;
use wg_2024::network::{NodeId, SourceRoutingHeader};
use wg_2024::packet::{FloodRequest, Nack, NackType, NodeType, Packet, PacketType};

/// drone implementation of the Rust&Furious group
pub struct RustAndFurious {
    // common data
    id: Arc<Mutex<NodeId>>,
    controller_send: Sender<DroneEvent>,
    controller_recv: Receiver<DroneCommand>,
    packet_recv: Receiver<Packet>,
    packet_send: HashMap<NodeId, Sender<Packet>>,
    pdr: f32,

    // group data
    received_flood_ids: HashSet<(NodeId, u64)>,
    is_in_crash_behaviour: bool
}

impl Debug for RustAndFurious {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let _ = f.debug_struct("RustAndFurious");
        write!(f, " {{\nid: {}\nneighbor ids:", self.id.lock().unwrap()).expect("unable to write drone");
        let mut neighbor_written = false;
        for (id, _) in &self.packet_send {
            write!(f, "\n\t- {}", id).expect("unable to write drone");
            neighbor_written = true;
        }
        if !neighbor_written {
            write!(f, "none").expect("unable to write drone");
        }
        write!(f, "\npdr: {}\n}}", self.pdr)
    }
}

impl Drone for RustAndFurious {
    fn new(
        id: NodeId,
        controller_send: Sender<DroneEvent>,
        controller_recv: Receiver<DroneCommand>,
        packet_recv: Receiver<Packet>,
        packet_send: HashMap<NodeId, Sender<Packet>>,
        pdr: f32
    ) -> Self {
        RustAndFurious {
            id: Arc::new(Mutex::new(id)),
            controller_send,
            controller_recv,
            packet_recv,
            packet_send,
            pdr,
            received_flood_ids: HashSet::new(),
            is_in_crash_behaviour: false
        }
    }

    fn run(&mut self) {
        loop {
            if !self.is_in_crash_behaviour {
                select_biased! {
                    recv(self.controller_recv) -> command => {
                        if let Ok(command) = command {
                            self.handle_command(command);
                        } else { // this shouldn't happen
                            panic!("controller_recv channel closed but drone isn't crashed");
                        }
                    },
                    recv(self.packet_recv) -> packet => {
                        if let Ok(packet) = packet {
                            self.process_packet(packet);
                        } else { // this shouldn't happen
                            panic!("packet_recv channel closed but drone isn't crashed");
                        }
                    }
                }
            } else {
                match self.packet_recv.try_recv() {
                    Ok(packet) => {
                        self.process_packet(packet);
                    },
                    Err(_) => break
                }
            }
        }
    }
}

impl RustAndFurious {
    fn handle_command(&mut self, command: DroneCommand) {
        match command {
            DroneCommand::RemoveSender(id) => { self.packet_send.remove(&id); },
            DroneCommand::AddSender(id, sender) => { self.packet_send.insert(id, sender); },
            DroneCommand::SetPacketDropRate(pdr) => self.pdr = pdr,
            DroneCommand::Crash => self.is_in_crash_behaviour = true
        }
    }
    fn process_packet(&mut self, packet: Packet) {
        match packet.clone().pack_type {
            PacketType::MsgFragment(ref fragment) => {
                if !self.is_in_crash_behaviour {
                    match self.perform_packet_checks(packet.clone()) {
                        Ok((mut packet, sender)) => {
                            if rng().random_bool(self.pdr as f64) {
                                packet.routing_header.decrease_hop_index();
                                self.send_nack(packet.clone(), NackType::Dropped, fragment.fragment_index);
                                self.controller_send.send(DroneEvent::PacketDropped(packet)).expect("controller_send channel closed but drone isn't crashed")
                            } else {
                                self.forward_packet(packet, sender);
                            }
                        },
                        Err((packet, nack_type)) => self.send_nack(packet, nack_type, fragment.fragment_index)
                    }
                } else {
                    let nack_type = NackType::ErrorInRouting(packet.routing_header.hops[packet.routing_header.hop_index]);
                    self.send_nack(packet, nack_type, fragment.fragment_index);
                }
            },
            PacketType::Nack(ref _nack) => {
                match self.perform_packet_checks(packet) {
                    Ok((packet, sender)) => self.forward_packet(packet, sender),
                    Err((packet, _)) => self.controller_send.send(DroneEvent::ControllerShortcut(packet)).expect("controller_send channel closed but drone isn't crashed")
                }
            },
            PacketType::Ack(ref _ack) => {
                match self.perform_packet_checks(packet) {
                    Ok((packet, sender)) => self.forward_packet(packet, sender),
                    Err((packet, _)) => self.controller_send.send(DroneEvent::ControllerShortcut(packet)).expect("controller_send channel closed but drone isn't crashed")
                }
            },
            PacketType::FloodRequest(flood_request) => if !self.is_in_crash_behaviour { self.handle_flood_request(flood_request) },
            PacketType::FloodResponse(ref _flood_response) => {
                match self.perform_packet_checks(packet) {
                    Ok((packet, sender)) => self.forward_packet(packet, sender),
                    Err((packet, _)) => self.controller_send.send(DroneEvent::ControllerShortcut(packet)).expect("controller_send channel closed but drone isn't crashed")
                }
            }
        }
    }

    /// performs all the checks on the packet, as described by the protocol
    fn perform_packet_checks(&self, mut packet: Packet) -> Result<(Packet, &Sender<Packet>), (Packet, NackType)> {
        // step 1
        if packet.routing_header.hops[packet.routing_header.hop_index] != *self.id.clone().lock().unwrap() {
            // step 1 error handling
            let nack_type = NackType::UnexpectedRecipient(*self.id.clone().lock().unwrap());
            return Err((packet, nack_type));
        }

        // step 3
        if packet.routing_header.is_last_hop() {
            // step 3 error handling
            let nack_type = NackType::DestinationIsDrone;
            return Err((packet, nack_type));
        }

        // step 2
        packet.routing_header.increase_hop_index();

        // step 4
        let sender_op = self.packet_send.get(&packet.routing_header.hops[packet.routing_header.hop_index]);
        if sender_op.is_none() {
            // step 4 error handling
            packet.routing_header.decrease_hop_index();
            let nack_type = NackType::ErrorInRouting(packet.routing_header.hops[packet.routing_header.hop_index]);
            return Err((packet, nack_type));
        }
        let sender = sender_op.unwrap();

        Ok((packet, sender))
    }

    /// get the SourceRoutingHeader to send back the packet to the sender through the path it used
    fn get_backwards_source_routing_header(&self, original: SourceRoutingHeader) -> SourceRoutingHeader {
        let tmp = original.hops.split_at(original.hop_index).0;
        let mut hops = Vec::new();
        hops.push(*self.id.clone().lock().unwrap());
        for id in tmp.iter().rev() {
            hops.push(*id);
        }

        SourceRoutingHeader::with_first_hop(hops)
    }
    /// sends back a Nack after an error occurred
    fn send_nack(&self, packet: Packet, nack_type: NackType, fragment_index: u64) {
        let sender_id = &packet.routing_header.hops[packet.routing_header.hop_index-1];
        let sender_op = self.packet_send.get(sender_id);
        if let Some(sender) = sender_op {
            let nack = Nack {
                fragment_index,
                nack_type
            };
            let routing_header = self.get_backwards_source_routing_header(packet.routing_header);

            let nack_response = Packet::new_nack(routing_header, packet.session_id, nack);
            self.forward_packet(nack_response, sender);
        } else { // the drone which sent that packet to the drone just crashed
            self.controller_send.send(DroneEvent::ControllerShortcut(packet)).expect("controller_send channel closed but drone isn't crashed");
        }
    }

    /// forwards the packet to the sender, without altering it
    fn forward_packet(&self, packet: Packet, sender: &Sender<Packet>) {
        match sender.send(packet.clone()) {
            Ok(_) => self.controller_send.send(DroneEvent::PacketSent(packet)).expect("controller_send channel closed but drone isn't crashed"),
            Err(error) => {
                let packet = error.0;
                match packet.clone().pack_type {
                    PacketType::MsgFragment(ref fragment) => {
                        let nack_type = NackType::ErrorInRouting(packet.routing_header.hops[packet.routing_header.hop_index-1]);
                        self.send_nack(packet, nack_type, fragment.fragment_index);
                    },
                    PacketType::Nack(ref _nack) => self.controller_send.send(DroneEvent::ControllerShortcut(packet)).expect("controller_send channel closed but drone isn't crashed"),
                    PacketType::Ack(ref _ack) => self.controller_send.send(DroneEvent::ControllerShortcut(packet)).expect("controller_send channel closed but drone isn't crashed"),
                    PacketType::FloodRequest(flood_request) => self.generate_and_send_flood_response(flood_request),
                    PacketType::FloodResponse(ref _flood_response) => self.controller_send.send(DroneEvent::ControllerShortcut(packet)).expect("controller_send channel closed but drone isn't crashed")
                }
            }
        }
    }

    fn generate_and_send_flood_response(&self, flood_request: FloodRequest) {
        let mut packet = flood_request.generate_response(rand::random::<u64>());
        packet.routing_header.increase_hop_index();

        let sender_op = self.packet_send.get(&packet.routing_header.hops[packet.routing_header.hop_index]);
        if sender_op.is_none() { // the drone which sent that packet to the drone just crashed
            self.controller_send.send(DroneEvent::ControllerShortcut(packet)).expect("controller_send channel closed but drone isn't crashed");
            return;
        }
        let sender = sender_op.unwrap();

        self.forward_packet(packet.clone(), sender);
    }
    fn handle_flood_request(&mut self, mut flood_request: FloodRequest) {
        if self.received_flood_ids.contains(&(*self.id.clone().lock().unwrap(), flood_request.flood_id)) { // flood already passed through this drone
            flood_request.increment(*self.id.clone().lock().unwrap(), NodeType::Drone);
            self.generate_and_send_flood_response(flood_request);
        } else { // flood not yet passed through this drone
            self.received_flood_ids.insert((*self.id.clone().lock().unwrap(), flood_request.flood_id));
            let last_flood_node = flood_request.path_trace.last().unwrap().0; // in theory when the flood gets here there has to be at least one element (the flood initiator)
            flood_request.increment(*self.id.clone().lock().unwrap(), NodeType::Drone);
            let mut flood_request_forwarded = false;

            // forward the flood request to other neighbors
            for (neighbor_id, neighbor_sender) in &self.packet_send {
                if *neighbor_id != last_flood_node {
                    flood_request_forwarded = true;
                    let packet = Packet::new_flood_request(SourceRoutingHeader::empty_route(), 0, flood_request.clone());
                    self.forward_packet(packet.clone(), neighbor_sender);
                }
            }

            // if there aren't any other neighbors
            if !flood_request_forwarded {
                self.generate_and_send_flood_response(flood_request);
            }
        }
    }
}

#[cfg(test)]
mod drone_tests {
    use super::*;
    use std::collections::HashMap;
    use std::{fs, thread};
    use std::thread::JoinHandle;
    use crossbeam_channel::{unbounded, Receiver, Sender};
    use wg_2024::config::Config;
    use wg_2024::controller::{DroneCommand, DroneEvent};
    use wg_2024::network::NodeId;
    use wg_2024::packet::{Ack, Fragment};
    //use wg_2024::tests as wg_tests;

    const CONFIG_FILE_PATH: &str = "src/config.toml";

    //#[test] fn wg_test1() { wg_tests::generic_fragment_forward::<RustAndFurious>(); }
    //#[test] fn wg_test2() { wg_tests::generic_fragment_drop::<RustAndFurious>(); }
    //#[test] fn wg_test3() { wg_tests::generic_chain_fragment_drop::<RustAndFurious>(); }
    //#[test] fn wg_test4() { wg_tests::generic_chain_fragment_ack::<RustAndFurious>(); }

    struct SimulationController {
        drones: HashMap<NodeId, Sender<DroneCommand>>,
        node_event_recv: Receiver<DroneEvent>,
    }
    impl SimulationController {
        fn crash_all(&mut self) {
            for (_, sender) in self.drones.iter() {
                sender.send(DroneCommand::Crash).unwrap();
            }
        }
    }
    fn get_test_drone() -> RustAndFurious {
        let id = rand::random::<u8>();
        let (controller_send, _) = unbounded();
        let (_, controller_recv) = unbounded();
        let (_, packet_recv) = unbounded();
        let packet_send = HashMap::new();
        let pdr = 1.0;
        RustAndFurious::new(id, controller_send, controller_recv, packet_recv, packet_send, pdr)
    }
    fn parse_config(file: &str) -> Config {
        let file_str = fs::read_to_string(file).expect("Unable to read config file");
        toml::from_str(&file_str).expect("Unable to parse TOML string data")
    }
    /// create msg fragment packet
    ///
    /// routing header: 1 -> (11) -> 12 -> 21
    ///
    /// fragment: index 1 out of 1 - data (1; 128)
    fn create_sample_packet() -> Packet {
        Packet {
            pack_type: PacketType::MsgFragment(Fragment {
                fragment_index: 1,
                total_n_fragments: 1,
                length: 128,
                data: [1; 128],
            }),
            routing_header: SourceRoutingHeader {
                hop_index: 1,
                hops: vec![1, 11, 12, 21],
            },
            session_id: 1,
        }
    }
    fn crash_drones_and_wait_join(mut controller: SimulationController, mut handles: Vec<JoinHandle<()>>) {
        controller.crash_all();
        while let Some(handle) = handles.pop() {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_get_backwards_source_routing_header() {
        let drone = get_test_drone();
        *drone.id.lock().unwrap() = 42;

        let original_header = SourceRoutingHeader {
            hop_index: 2,
            hops: vec![10, 20, 30, 40],
        };

        let result = drone.get_backwards_source_routing_header(original_header.clone());

        assert_eq!(result.hop_index, 1);
        assert_eq!(result.hops, vec![42, 20, 10]);
    }

    #[test]
    fn test_drop_rate() {
        let drone = get_test_drone();

        let mut count = 0;
        for _i in 0..100 {
            if rng().random_bool(drone.pdr as f64) {
                count += 1;
            }
        }
        println!("n°: {}", count);
    }

    #[test]
    fn test_drones_crash() {
        let config = parse_config(CONFIG_FILE_PATH);

        let mut controller_drones = HashMap::new();
        let (node_event_send, node_event_recv) = unbounded();

        let mut packet_channels = HashMap::new();
        for drone in config.drone.iter() {
            packet_channels.insert(drone.id, unbounded());
        }
        for client in config.client.iter() {
            packet_channels.insert(client.id, unbounded());
        }
        for server in config.server.iter() {
            packet_channels.insert(server.id, unbounded());
        }

        let mut handles = Vec::new();
        for drone in config.drone.into_iter() {
            // controller
            let (controller_drone_send, controller_drone_recv) = unbounded();
            controller_drones.insert(drone.id, controller_drone_send);
            let node_event_send = node_event_send.clone();
            // packet
            let packet_recv = packet_channels[&drone.id].1.clone();
            let packet_send = drone
                .connected_node_ids
                .into_iter()
                .map(|id| (id, packet_channels[&id].0.clone()))
                .collect();

            handles.push(thread::spawn(move || {
                let mut drone = RustAndFurious::new(
                    drone.id,
                    node_event_send,
                    controller_drone_recv,
                    packet_recv,
                    packet_send,
                    drone.pdr,
                );

                drone.run();
            }));
        }

        let controller = SimulationController {
            drones: controller_drones,
            node_event_recv,
        };

        crash_drones_and_wait_join(controller, handles);
    }

    #[test]
    pub fn generic_fragment_forward() {
        let mut controller_drones = HashMap::new();
        let (node_event_send, node_event_recv) = unbounded();
        let mut handles = Vec::new();

        // drone 2 <Packet>
        let (d_send, d_recv) = unbounded();
        // drone 3 <Packet>
        let (d2_send, d2_recv) = unbounded::<Packet>();
        // SC commands
        let (d_command_send, d_command_recv) = unbounded();

        let neighbours = HashMap::from([(12, d2_send.clone())]);
        controller_drones.insert(11, d_command_send);
        let mut drone = RustAndFurious::new(
            11,
            node_event_send.clone(),
            d_command_recv,
            d_recv.clone(),
            neighbours,
            0.0,
        );
        // Spawn the drone's run method in a separate thread
        handles.push(thread::spawn(move || {
            drone.run();
        }));

        let mut msg = create_sample_packet();

        // "Client" sends packet to d
        d_send.send(msg.clone()).unwrap();
        msg.routing_header.hop_index = 2;

        // d2 receives packet from d1
        assert_eq!(d2_recv.recv().unwrap(), msg);

        let controller = SimulationController {
            drones: controller_drones,
            node_event_recv,
        };
        crash_drones_and_wait_join(controller, handles);
    }

    #[test]
    pub fn generic_fragment_drop() {
        let mut controller_drones = HashMap::new();
        let (node_event_send, node_event_recv) = unbounded();
        let mut handles = Vec::new();

        // Client 1
        let (c_send, c_recv) = unbounded();
        // Drone 11
        let (d_send11, d_recv11) = unbounded();
        // Drone 12
        let (d_send12, _d_recv12) = unbounded();
        // SC commands
        let (d_command_send, d_command_recv) = unbounded();

        controller_drones.insert(11, d_command_send);
        let neighbours = HashMap::from([(12, d_send12.clone()), (1, c_send.clone())]);
        let mut drone = RustAndFurious::new(
            11,
            node_event_send.clone(),
            d_command_recv,
            d_recv11,
            neighbours,
            1.0,
        );

        // Spawn the drone's run method in a separate thread
        handles.push(thread::spawn(move || {
            drone.run();
        }));

        let msg = create_sample_packet();

        // "Client" sends packet to the drone
        d_send11.send(msg.clone()).unwrap();

        let dropped = Nack {
            fragment_index: 1,
            nack_type: NackType::Dropped,
        };
        let srh = SourceRoutingHeader {
            hop_index: 1,
            hops: vec![11, 1],
        };
        let nack_packet = Packet {
            pack_type: PacketType::Nack(dropped),
            routing_header: srh,
            session_id: 1,
        };

        // Client listens for packet from the drone (Dropped Nack)
        assert_eq!(c_recv.recv().unwrap(), nack_packet);

        let controller = SimulationController {
            drones: controller_drones,
            node_event_recv,
        };
        crash_drones_and_wait_join(controller, handles);
    }

    #[test]
    pub fn generic_chain_fragment_drop() {
        let mut controller_drones = HashMap::new();
        let (node_event_send, node_event_recv) = unbounded();
        let mut handles = Vec::new();

        // Client 1 channels
        let (c_send, c_recv) = unbounded();
        // Server 21 channels
        let (s_send, _s_recv) = unbounded();
        // Drone 11
        let (d_send, d_recv) = unbounded();
        // Drone 12
        let (d12_send, d12_recv) = unbounded();
        // SC - needed to not make the drone crash
        let (controller_drone_send11, controller_drone_recv11) = unbounded();
        let (controller_drone_send12, controller_drone_recv12) = unbounded();

        // Drone 11
        controller_drones.insert(11, controller_drone_send11);
        let neighbours11 = HashMap::from([(12, d12_send.clone()), (1, c_send.clone())]);
        let mut drone1 = RustAndFurious::new(
            11,
            node_event_send.clone(),
            controller_drone_recv11,
            d_recv.clone(),
            neighbours11,
            0.0,
        );
        // Drone 12
        controller_drones.insert(12, controller_drone_send12);
        let neighbours12 = HashMap::from([(11, d_send.clone()), (21, s_send.clone())]);
        let mut drone2 = RustAndFurious::new(
            12,
            node_event_send.clone(),
            controller_drone_recv12,
            d12_recv.clone(),
            neighbours12,
            1.0,
        );

        // Spawn the drone's run method in a separate thread
        handles.push(thread::spawn(move || {
            drone1.run();
        }));

        handles.push(thread::spawn(move || {
            drone2.run();
        }));

        let msg = create_sample_packet();

        // "Client" sends packet to the drone
        d_send.send(msg.clone()).unwrap();

        // Client receive an NACK originated from 'd2'
        assert_eq!(
            c_recv.recv().unwrap(),
            Packet {
                pack_type: PacketType::Nack(Nack {
                    fragment_index: 1,
                    nack_type: NackType::Dropped,
                }),
                routing_header: SourceRoutingHeader {
                    hop_index: 2,
                    hops: vec![12, 11, 1],
                },
                session_id: 1,
            }
        );

        let controller = SimulationController {
            drones: controller_drones,
            node_event_recv,
        };
        crash_drones_and_wait_join(controller, handles);
    }

    #[test]
    pub fn generic_chain_fragment_ack() {
        let mut controller_drones = HashMap::new();
        let (node_event_send, node_event_recv) = unbounded();
        let mut handles = Vec::new();

        // Client<1> channels
        let (c_send, c_recv) = unbounded();
        // Server<21> channels
        let (s_send, s_recv) = unbounded();
        // Drone 11
        let (d_send, d_recv) = unbounded();
        // Drone 12
        let (d12_send, d12_recv) = unbounded();

        // Drone 11
        let (controller_drone_send11, controller_drone_recv11) = unbounded();
        controller_drones.insert(11, controller_drone_send11);
        let neighbours11 = HashMap::from([(12, d12_send.clone()), (1, c_send.clone())]);
        let mut drone = RustAndFurious::new(
            11,
            node_event_send.clone(),
            controller_drone_recv11,
            d_recv.clone(),
            neighbours11,
            0.0,
        );
        // Drone 12
        let (controller_drone_send12, controller_drone_recv12) = unbounded();
        controller_drones.insert(12, controller_drone_send12);
        let neighbours12 = HashMap::from([(11, d_send.clone()), (21, s_send.clone())]);
        let mut drone2 = RustAndFurious::new(
            12,
            node_event_send.clone(),
            controller_drone_recv12,
            d12_recv.clone(),
            neighbours12,
            0.0,
        );

        // Spawn the drone's run method in a separate thread
        handles.push(thread::spawn(move || {
            drone.run();
        }));

        handles.push(thread::spawn(move || {
            drone2.run();
        }));

        let mut msg = create_sample_packet();

        // "Client" sends packet to the drone
        d_send.send(msg.clone()).unwrap();

        // "Server" receives the packet that has been sent
        msg.routing_header.hop_index = 3;
        // Server receives the fragment
        assert_eq!(s_recv.recv().unwrap(), msg);

        // "Server" sends ack to "Client"
        let mut ack = Packet {
            pack_type: PacketType::Ack(Ack { fragment_index: 1 }),
            routing_header: SourceRoutingHeader {
                hop_index: 1,
                hops: vec![21, 12, 11, 1],
            },
            session_id: 1,
        };
        d12_send.send(ack.clone()).unwrap();

        ack.routing_header.hop_index = 3;

        // "Client" receive an ACK originated from "Server"
        assert_eq!(c_recv.recv().unwrap(), ack);

        let controller = SimulationController {
            drones: controller_drones,
            node_event_recv,
        };
        crash_drones_and_wait_join(controller, handles);
    }
}