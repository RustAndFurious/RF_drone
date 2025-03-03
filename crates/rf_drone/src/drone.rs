use crossbeam_channel::{select_biased, Receiver, Sender};
use rand::{rng, Rng};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use wg_2024::controller::{DroneCommand, DroneEvent};
use wg_2024::drone::Drone;
use wg_2024::network::{NodeId, SourceRoutingHeader};
use wg_2024::packet::{FloodRequest, Nack, NackType, NodeType, Packet, PacketType};

/// drone implementation of the Rust&Furious group
pub struct RustAndFurious {
    // common data
    id: u8,
    controller_send: Sender<DroneEvent>,
    controller_recv: Receiver<DroneCommand>,
    packet_recv: Receiver<Packet>,
    packet_send: HashMap<NodeId, Sender<Packet>>,
    pdr: f32,

    // group data
    received_flood_ids: HashSet<(NodeId, u64)>,
    is_in_crash_behaviour: bool,
}

impl Debug for RustAndFurious {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let _ = f.debug_struct("RustAndFurious");
        write!(f, " {{\nid: {}\nneighbor ids:", self.id).expect("unable to write drone");
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
        pdr: f32,
    ) -> Self {
        RustAndFurious {
            id,
            controller_send,
            controller_recv,
            packet_recv,
            packet_send,
            pdr,
            received_flood_ids: HashSet::new(),
            is_in_crash_behaviour: false,
        }
    }

    fn run(&mut self) {
        while !self.is_in_crash_behaviour {
            select_biased! {
                recv(self.controller_recv) -> command => {
                    if let Ok(command) = command {
                        self.handle_command(command);
                    } else { // this shouldn't happen
                        panic!("{}: controller_recv channel closed but drone isn't crashed", self.id);
                    }
                },
                recv(self.packet_recv) -> packet => {
                    if let Ok(packet) = packet {
                        self.process_packet(packet);
                    }
                }
            }
        }
        loop {
            match self.packet_recv.recv() {
                Ok(packet) => {
                    self.process_packet(packet);
                }
                Err(_) => break,
            }
        }
    }
}

impl RustAndFurious {
    fn handle_command(&mut self, command: DroneCommand) {
        match command {
            DroneCommand::RemoveSender(id) => {
                self.packet_send.remove(&id);
            }
            DroneCommand::AddSender(id, sender) => {
                self.packet_send.insert(id, sender);
            }
            DroneCommand::SetPacketDropRate(pdr) => self.pdr = pdr,
            DroneCommand::Crash => self.is_in_crash_behaviour = true,
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
                                self.controller_send
                                    .send(DroneEvent::PacketDropped(packet.clone()))
                                    .expect(
                                        "controller_send channel closed but drone isn't crashed",
                                    );
                                self.send_nack(packet, NackType::Dropped, fragment.fragment_index);
                            } else {
                                self.forward_packet(packet, sender);
                            }
                        }
                        Err((packet, nack_type)) => {
                            self.send_nack(packet, nack_type, fragment.fragment_index)
                        }
                    }
                } else {
                    let nack_type = NackType::ErrorInRouting(
                        packet.routing_header.hops[packet.routing_header.hop_index],
                    );
                    self.send_nack(packet, nack_type, fragment.fragment_index);
                }
            }
            PacketType::Nack(ref _nack) => match self.perform_packet_checks(packet) {
                Ok((packet, sender)) => self.forward_packet(packet, sender),
                Err((packet, _)) => self
                    .controller_send
                    .send(DroneEvent::ControllerShortcut(packet))
                    .expect("controller_send channel closed but drone isn't crashed"),
            },
            PacketType::Ack(ref _ack) => match self.perform_packet_checks(packet) {
                Ok((packet, sender)) => self.forward_packet(packet, sender),
                Err((packet, _)) => self
                    .controller_send
                    .send(DroneEvent::ControllerShortcut(packet))
                    .expect("controller_send channel closed but drone isn't crashed"),
            },
            PacketType::FloodRequest(flood_request) => {
                if !self.is_in_crash_behaviour {
                    self.handle_flood_request(flood_request, packet.session_id)
                }
            }
            PacketType::FloodResponse(ref _flood_response) => {
                match self.perform_packet_checks(packet) {
                    Ok((packet, sender)) => self.forward_packet(packet, sender),
                    Err((packet, _)) => self
                        .controller_send
                        .send(DroneEvent::ControllerShortcut(packet))
                        .expect("controller_send channel closed but drone isn't crashed"),
                }
            }
        }
    }

    /// performs all the checks on the packet, as described by the protocol
    fn perform_packet_checks(
        &self,
        mut packet: Packet,
    ) -> Result<(Packet, &Sender<Packet>), (Packet, NackType)> {
        // step 1
        if packet.routing_header.hops[packet.routing_header.hop_index] != self.id {
            // step 1 error handling
            let nack_type = NackType::UnexpectedRecipient(self.id);
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
        let sender_op = self
            .packet_send
            .get(&packet.routing_header.hops[packet.routing_header.hop_index]);
        if sender_op.is_none() {
            // step 4 error handling
            let nack_type = NackType::ErrorInRouting(
                packet.routing_header.hops[packet.routing_header.hop_index],
            );
            return Err((packet, nack_type));
        }
        let sender = sender_op.unwrap();

        Ok((packet, sender))
    }

    /// get the SourceRoutingHeader to send back the packet to the sender through the path it used
    fn get_backwards_source_routing_header(
        &self,
        original: SourceRoutingHeader,
    ) -> SourceRoutingHeader {
        let tmp = original.hops.split_at(original.hop_index).0;
        let mut hops = Vec::new();
        hops.push(self.id);
        for id in tmp.iter().rev() {
            hops.push(*id);
        }

        SourceRoutingHeader::with_first_hop(hops)
    }
    /// sends back a Nack after an error occurred
    fn send_nack(&self, mut packet: Packet, nack_type: NackType, fragment_index: u64) {
        let sender_id = &packet.routing_header.hops[packet.routing_header.hop_index - 1];
        let sender_op = self.packet_send.get(sender_id);
        if let Some(sender) = sender_op {
            let nack = Nack {
                fragment_index,
                nack_type,
            };
            while packet.routing_header.hops.len() > packet.routing_header.hop_index {
                packet.routing_header.hops.pop();
            }
            let routing_header = self.get_backwards_source_routing_header(packet.routing_header);

            let nack_response = Packet::new_nack(routing_header, packet.session_id, nack);
            self.forward_packet(nack_response, sender);
        } else {
            // the drone which sent that packet to the drone just crashed
            self.controller_send
                .send(DroneEvent::ControllerShortcut(packet))
                .expect("controller_send channel closed but drone isn't crashed");
        }
    }

    /// forwards the packet to the sender, without altering it
    fn forward_packet(&self, packet: Packet, sender: &Sender<Packet>) {
        match sender.send(packet.clone()) {
            Ok(_) => self
                .controller_send
                .send(DroneEvent::PacketSent(packet))
                .expect("controller_send channel closed but drone isn't crashed"),
            Err(error) => {
                let packet = error.0;
                match packet.clone().pack_type {
                    PacketType::MsgFragment(ref fragment) => {
                        let nack_type = NackType::ErrorInRouting(
                            packet.routing_header.hops[packet.routing_header.hop_index - 1],
                        );
                        self.send_nack(packet, nack_type, fragment.fragment_index);
                    }
                    PacketType::Nack(ref _nack) => self
                        .controller_send
                        .send(DroneEvent::ControllerShortcut(packet))
                        .expect("controller_send channel closed but drone isn't crashed"),
                    PacketType::Ack(ref _ack) => self
                        .controller_send
                        .send(DroneEvent::ControllerShortcut(packet))
                        .expect("controller_send channel closed but drone isn't crashed"),
                    PacketType::FloodRequest(flood_request) => {
                        self.generate_and_send_flood_response(flood_request, packet.session_id)
                    }
                    PacketType::FloodResponse(ref _flood_response) => self
                        .controller_send
                        .send(DroneEvent::ControllerShortcut(packet))
                        .expect("controller_send channel closed but drone isn't crashed"),
                }
            }
        }
    }

    fn generate_and_send_flood_response(&self, flood_request: FloodRequest, session_id: u64) {
        let mut packet = flood_request.generate_response(session_id);
        packet.routing_header.increase_hop_index();

        let sender_op = self
            .packet_send
            .get(&packet.routing_header.hops[packet.routing_header.hop_index]);
        if sender_op.is_none() {
            // the drone which sent that packet to the drone just crashed
            self.controller_send
                .send(DroneEvent::ControllerShortcut(packet))
                .expect("controller_send channel closed but drone isn't crashed");
            return;
        }
        let sender = sender_op.unwrap();

        self.forward_packet(packet, sender);
    }
    fn handle_flood_request(&mut self, mut flood_request: FloodRequest, session_id: u64) {
        if self
            .received_flood_ids
            .insert((flood_request.initiator_id, flood_request.flood_id))
        {
            // flood not yet passed through this drone
            let last_flood_node = match flood_request.path_trace.last() {
                Some((node_id, _)) => node_id,
                None => &flood_request.initiator_id,
            };
            let mut flood_request = flood_request.clone();
            flood_request.increment(self.id, NodeType::Drone);
            let mut flood_request_forwarded = false;

            // forward the flood request to other neighbors
            for (neighbor_id, neighbor_sender) in &self.packet_send {
                if neighbor_id != last_flood_node {
                    flood_request_forwarded = true;
                    let packet = Packet::new_flood_request(
                        SourceRoutingHeader::empty_route(),
                        0,
                        flood_request.clone(),
                    );
                    self.forward_packet(packet.clone(), neighbor_sender);
                }
            }

            // if there aren't any other neighbors
            if !flood_request_forwarded {
                self.generate_and_send_flood_response(flood_request, session_id);
            }
        } else {
            // flood already passed through this drone
            flood_request.increment(self.id, NodeType::Drone);
            self.generate_and_send_flood_response(flood_request, session_id);
        }
    }
}

#[cfg(test)]
mod drone_tests {
    use super::*;
    use crossbeam_channel::{unbounded, Receiver, Sender};
    use std::collections::HashMap;
    use std::thread::JoinHandle;
    use std::{fs, thread};
    use wg_2024::config::Config;
    use wg_2024::controller::{DroneCommand, DroneEvent};
    use wg_2024::network::NodeId;
    use wg_2024::packet::{Ack, FloodResponse, Fragment};

    struct SimulationController {
        drones: HashMap<NodeId, Sender<DroneCommand>>,
        _node_event_recv: Receiver<DroneEvent>,
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
        RustAndFurious::new(
            id,
            controller_send,
            controller_recv,
            packet_recv,
            packet_send,
            pdr,
        )
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
    fn crash_drones_and_wait_join(
        mut controller: SimulationController,
        drone_senders: Vec<Sender<Packet>>,
        config: Config,
        mut handles: Vec<JoinHandle<()>>,
    ) {
        for drone_sender in drone_senders {
            drop(drone_sender);
        }
        for drone in config.drone {
            for id in drone.connected_node_ids {
                controller
                    .drones
                    .get(&drone.id)
                    .unwrap()
                    .send(DroneCommand::RemoveSender(id))
                    .unwrap()
            }
        }
        controller.crash_all();
        while let Some(handle) = handles.pop() {
            handle.join().unwrap();
        }
    }

    #[test]
    fn get_backwards_source_routing_header() {
        let mut drone = get_test_drone();
        drone.id = 42;

        let original_header = SourceRoutingHeader {
            hop_index: 2,
            hops: vec![10, 20, 30, 40],
        };

        let result = drone.get_backwards_source_routing_header(original_header.clone());

        assert_eq!(result.hop_index, 1);
        assert_eq!(result.hops, vec![42, 20, 10]);
    }

    #[test]
    fn drop_rate() {
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
    fn drones_crash() {
        let config = parse_config("src/test_configs/config_crash.toml");

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
        for drone in config.drone.clone().into_iter() {
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
            _node_event_recv: node_event_recv,
        };

        drop(packet_channels);
        crash_drones_and_wait_join(controller, Vec::new(), config, handles);
    }

    #[test]
    fn fragment_forward() {
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

        // controller receives packet forward event from d1
        let event = DroneEvent::PacketSent(msg.clone());
        assert_eq!(node_event_recv.recv().unwrap(), event);

        let controller = SimulationController {
            drones: controller_drones,
            _node_event_recv: node_event_recv,
        };

        let senders = vec![d_send, d2_send];
        crash_drones_and_wait_join(
            controller,
            senders,
            Config {
                drone: vec![wg_2024::config::Drone {
                    id: 11,
                    connected_node_ids: vec![12],
                    pdr: 0.0,
                }],
                client: vec![],
                server: vec![],
            },
            handles,
        );
    }

    #[test]
    fn fragment_drop() {
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

        // controller receives the PacketDropped event
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketDropped(msg)
        );
        // controller receives the PacketSent event about the nack packet
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketSent(nack_packet)
        );

        let controller = SimulationController {
            drones: controller_drones,
            _node_event_recv: node_event_recv,
        };
        let drone_senders = Vec::from([d_send11, d_send12]);
        crash_drones_and_wait_join(
            controller,
            drone_senders,
            Config {
                drone: vec![wg_2024::config::Drone {
                    id: 11,
                    connected_node_ids: vec![1, 12],
                    pdr: 0.0,
                }],
                client: vec![],
                server: vec![],
            },
            handles,
        );
    }

    #[test]
    fn chain_fragment_drop() {
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

        let mut msg = create_sample_packet();

        // "Client" sends packet to the drone
        d_send.send(msg.clone()).unwrap();

        // Client receive an NACK originated from 'd2'
        let mut nack_packet = Packet {
            pack_type: PacketType::Nack(Nack {
                fragment_index: 1,
                nack_type: NackType::Dropped,
            }),
            routing_header: SourceRoutingHeader {
                hop_index: 2,
                hops: vec![12, 11, 1],
            },
            session_id: 1,
        };
        assert_eq!(c_recv.recv().unwrap(), nack_packet);

        // controller receives PacketSent event from d1
        msg.routing_header.increase_hop_index();
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketSent(msg.clone())
        );
        // controller receives PacketDropped event from d2
        nack_packet.routing_header.hop_index = 1;
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketDropped(msg)
        );
        // controller receives PacketSent event from d2 about the nack packet
        nack_packet.routing_header.hop_index = 1;
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketSent(nack_packet.clone())
        );
        // controller receives PacketSent event from d1 about the nack packet
        nack_packet.routing_header.hop_index = 2;
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketSent(nack_packet)
        );

        let controller = SimulationController {
            drones: controller_drones,
            _node_event_recv: node_event_recv,
        };

        let senders = vec![d_send, d12_send];
        crash_drones_and_wait_join(
            controller,
            senders,
            Config {
                drone: vec![
                    wg_2024::config::Drone {
                        id: 11,
                        connected_node_ids: vec![12, 1],
                        pdr: 0.0,
                    },
                    wg_2024::config::Drone {
                        id: 12,
                        connected_node_ids: vec![11, 21],
                        pdr: 0.0,
                    },
                ],
                client: vec![],
                server: vec![],
            },
            handles,
        );
    }

    #[test]
    fn chain_fragment_ack() {
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

        // controller receives PacketSent event from d1
        msg.routing_header.increase_hop_index();
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketSent(msg.clone())
        );
        // controller receives PacketSent event from d2
        msg.routing_header.increase_hop_index();
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketSent(msg.clone())
        );

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

        // controller receives PacketSent event from d1
        ack.routing_header.increase_hop_index();
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketSent(ack.clone())
        );
        // controller receives PacketSent event from d2
        ack.routing_header.increase_hop_index();
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::PacketSent(ack.clone())
        );

        // "Client" receive an ACK originated from "Server"
        assert_eq!(c_recv.recv().unwrap(), ack);

        let controller = SimulationController {
            drones: controller_drones,
            _node_event_recv: node_event_recv,
        };

        let senders = vec![d_send, d12_send];
        crash_drones_and_wait_join(
            controller,
            senders,
            Config {
                drone: vec![
                    wg_2024::config::Drone {
                        id: 11,
                        connected_node_ids: vec![12, 1],
                        pdr: 0.0,
                    },
                    wg_2024::config::Drone {
                        id: 12,
                        connected_node_ids: vec![11, 21],
                        pdr: 0.0,
                    },
                ],
                client: vec![],
                server: vec![],
            },
            handles,
        );
    }

    #[test]
    fn forward_important_packet_trough_controller() {
        let mut controller_drones = HashMap::new();
        let (node_event_send, node_event_recv) = unbounded();
        let mut handles = Vec::new();

        // drone <Packet>
        let (d_send, d_recv) = unbounded();
        // other casual drone
        let (d_send2, _) = unbounded();
        // SC commands
        let (d_command_send, d_command_recv) = unbounded();

        let neighbours = HashMap::from([(80, d_send2.clone())]);
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

        // packets to send through drone
        let ack = Packet {
            pack_type: PacketType::Ack(Ack { fragment_index: 1 }),
            routing_header: SourceRoutingHeader {
                hop_index: 1,
                hops: vec![21, 12, 13, 1],
            },
            session_id: 16,
        };
        let nack = Packet {
            pack_type: PacketType::Nack(Nack {
                fragment_index: 1,
                nack_type: NackType::ErrorInRouting(41),
            }),
            routing_header: SourceRoutingHeader {
                hop_index: 2,
                hops: vec![12, 11, 1],
            },
            session_id: 5,
        };
        let flood_response = Packet {
            pack_type: PacketType::FloodResponse(FloodResponse {
                flood_id: 0,
                path_trace: vec![
                    (12, NodeType::Drone),
                    (15, NodeType::Drone),
                    (16, NodeType::Drone),
                    (13, NodeType::Drone),
                    (49, NodeType::Drone),
                    (7, NodeType::Drone),
                    (14, NodeType::Server),
                ],
            }),
            routing_header: SourceRoutingHeader {
                hop_index: 5,
                hops: vec![12, 15, 16, 13, 49, 7, 14],
            },
            session_id: 19,
        };

        // sends the packets
        d_send.send(ack.clone()).unwrap();
        d_send.send(nack.clone()).unwrap();
        d_send.send(flood_response.clone()).unwrap();

        // controller should receive the packets as ControllerShortcut
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::ControllerShortcut(ack)
        );
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::ControllerShortcut(nack)
        );
        assert_eq!(
            node_event_recv.recv().unwrap(),
            DroneEvent::ControllerShortcut(flood_response)
        );

        let controller = SimulationController {
            drones: controller_drones,
            _node_event_recv: node_event_recv,
        };

        let senders = vec![d_send, d_send2];
        crash_drones_and_wait_join(
            controller,
            senders,
            Config {
                drone: vec![wg_2024::config::Drone {
                    id: 11,
                    connected_node_ids: vec![80],
                    pdr: 0.0,
                }],
                client: vec![],
                server: vec![],
            },
            handles,
        );
    }

    #[test]
    fn flood() {
        fn build_topology(
            self_id: NodeId,
            path_traces: Vec<Vec<(NodeId, NodeType)>>,
        ) -> HashMap<NodeId, HashSet<NodeId>> {
            let mut topology = HashMap::new();

            for path_trace in path_traces {
                let mut last = self_id;
                if topology.get(&last).is_none() {
                    topology.insert(last, HashSet::new());
                }
                for (node_id, _) in path_trace {
                    topology.get_mut(&last).unwrap().insert(node_id);
                    if topology.get(&node_id).is_none() {
                        topology.insert(node_id, HashSet::new());
                    }
                    topology.get_mut(&node_id).unwrap().insert(last);
                    last = node_id;
                }
            }

            topology
        }

        let config = parse_config("src/test_configs/config_flood.toml");

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
        for drone in config.drone.clone().into_iter() {
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
            _node_event_recv: node_event_recv,
        };

        // spawn thread for servers
        let server_recv = packet_channels[&21].1.clone();
        let drone_send = packet_channels[&4].0.clone();
        handles.push(thread::spawn(move || {
            let recv = server_recv.recv().unwrap();
            //println!("server 21 received something");
            assert!(match recv.clone().pack_type {
                PacketType::FloodRequest(mut flood_request) => {
                    flood_request.increment(21, NodeType::Server);
                    let mut packet = flood_request.generate_response(recv.session_id);
                    packet.routing_header.increase_hop_index();
                    //println!("server 21 sending: {}", packet.clone());
                    drone_send.send(packet).unwrap();
                    true
                }
                _ => false,
            });
        }));
        let server_recv = packet_channels[&22].1.clone();
        let drone_send = packet_channels[&3].0.clone();
        handles.push(thread::spawn(move || {
            let recv = server_recv.recv().unwrap();
            //println!("server 22 received something");
            assert!(match recv.clone().pack_type {
                PacketType::FloodRequest(mut flood_request) => {
                    flood_request.increment(22, NodeType::Server);
                    let mut packet = flood_request.generate_response(recv.session_id);
                    packet.routing_header.increase_hop_index();
                    //println!("server 22 sending: {}", packet.clone());
                    drone_send.send(packet).unwrap();
                    true
                }
                _ => false,
            });
        }));

        // client sends the flood request to d1
        let flood_request = Packet::new_flood_request(
            SourceRoutingHeader::empty_route(),
            rand::random::<u64>(),
            FloodRequest::new(rand::random::<u64>(), 11),
        );
        let drone_send = packet_channels[&1].0.clone();
        drone_send.send(flood_request).unwrap();

        // client receives two flood response with the paths to se two servers
        let mut path_traces = Vec::new();
        let client_recv = packet_channels[&11].1.clone();
        for _ in 0..4 {
            assert!(match client_recv.recv().unwrap().pack_type {
                PacketType::FloodResponse(flood_response) => {
                    //println!("{:?}", &flood_response.path_trace);
                    path_traces.push(flood_response.path_trace);
                    true
                }
                _ => false,
            });
        }
        let mut topology: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();
        topology.insert(11, HashSet::from([1]));
        topology.insert(1, HashSet::from([11, 2, 3]));
        topology.insert(2, HashSet::from([1, 4]));
        topology.insert(3, HashSet::from([1, 22, 4]));
        topology.insert(4, HashSet::from([2, 21, 3]));
        topology.insert(21, HashSet::from([4]));
        topology.insert(22, HashSet::from([3]));
        assert_eq!(topology, build_topology(11, path_traces));

        drop(packet_channels);
        drop(drone_send);
        crash_drones_and_wait_join(controller, Vec::new(), config, handles);
    }
}
