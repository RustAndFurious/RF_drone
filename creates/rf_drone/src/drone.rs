use crossbeam_channel::{select_biased, Receiver, SendError, Sender};
use std::collections::{HashMap, HashSet};
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
                match self.packet_recv.recv() {
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
        match packet.pack_type {
            PacketType::MsgFragment(ref _fragment) => {
                if !self.is_in_crash_behaviour {
                    match self.perform_packet_checks(packet) {
                        Ok((packet, sender)) => {
                            if rng().random_bool(self.pdr as f64) {
                                self.send_nack(packet.clone(), NackType::Dropped);
                                self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::PacketDropped(packet)));
                            } else {
                                self.forward_packet(packet, sender);
                            }
                        },
                        Err((packet, nack_type)) => self.send_nack(packet, nack_type)
                    }
                } else {
                    let nack_type = NackType::ErrorInRouting(packet.routing_header.hops[packet.routing_header.hop_index]);
                    self.send_nack(packet, nack_type);
                }
            },
            PacketType::Nack(ref _nack) => {
                match self.perform_packet_checks(packet) {
                    Ok((packet, sender)) => self.forward_packet(packet, sender),
                    Err((packet, _)) => self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::ControllerShortcut(packet)))
                }
            },
            PacketType::Ack(ref _ack) => {
                match self.perform_packet_checks(packet) {
                    Ok((packet, sender)) => self.forward_packet(packet, sender),
                    Err((packet, _)) => self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::ControllerShortcut(packet)))
                }
            },
            PacketType::FloodRequest(flood_request) => if !self.is_in_crash_behaviour { self.handle_flood_request(flood_request) },
            PacketType::FloodResponse(ref _flood_response) => {
                match self.perform_packet_checks(packet) {
                    Ok((packet, sender)) => self.forward_packet(packet, sender),
                    Err((packet, _)) => self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::ControllerShortcut(packet)))
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

        // step 2
        packet.routing_header.increase_hop_index();

        // step 3
        if packet.routing_header.is_last_hop() {
            // step 3 error handling
            let nack_type = NackType::DestinationIsDrone;
            return Err((packet, nack_type));
        }

        // step 4
        let sender_op = self.packet_send.get(&packet.routing_header.hops[packet.routing_header.hop_index]);
        if sender_op.is_none() {
            // step 4 error handling
            let nack_type = NackType::ErrorInRouting(packet.routing_header.hops[packet.routing_header.hop_index-1]);
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
    fn send_nack(&self, packet: Packet, nack_type: NackType) {
        let sender_id = &packet.routing_header.hops[packet.routing_header.hop_index-1];
        let sender_op = self.packet_send.get(sender_id);
        if let Some(sender) = sender_op {
            let nack = Nack {
                fragment_index: 0,
                nack_type
            };
            let routing_header = self.get_backwards_source_routing_header(packet.routing_header);
            let session_id = rand::random::<u64>();

            let nack_response = Packet::new_nack(routing_header, session_id, nack);
            self.forward_packet(nack_response, sender);
        } else { // the drone which sent that packet to the drone just crashed
            self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::ControllerShortcut(packet)));
        }
    }

    /// forwards the packet to the sender, without altering it
    fn forward_packet(&self, packet: Packet, sender: &Sender<Packet>) {
        match sender.send(packet.clone()) {
            Ok(_) => self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::PacketSent(packet))),
            Err(error) => {
                let packet = error.0;
                match packet.pack_type {
                    PacketType::MsgFragment(ref _fragment) => {
                        let nack_type = NackType::ErrorInRouting(packet.routing_header.hops[packet.routing_header.hop_index-1]);
                        self.send_nack(packet, nack_type);
                    },
                    PacketType::Nack(ref _nack) => self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::ControllerShortcut(packet))),
                    PacketType::Ack(ref _ack) => self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::ControllerShortcut(packet))),
                    PacketType::FloodRequest(flood_request) => self.generate_and_send_flood_response(flood_request),
                    PacketType::FloodResponse(ref _flood_response) => self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::ControllerShortcut(packet)))
                }
            }
        }
    }

    fn generate_and_send_flood_response(&self, flood_request: FloodRequest) {
        let mut packet = flood_request.generate_response(rand::random::<u64>());
        packet.routing_header.increase_hop_index();

        let sender_op = self.packet_send.get(&packet.routing_header.hops[packet.routing_header.hop_index]);
        if sender_op.is_none() { // the drone which sent that packet to the drone just crashed
            self.execute_send_to_controller_recv(self.controller_send.send(DroneEvent::ControllerShortcut(packet)));
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

    /// execute the sending through controller_recv, panicking in case of an error
    fn execute_send_to_controller_recv<T>(&self, send: Result<(), SendError<T>>) {
        match send {
            Ok(_) => {},
            Err(_) => panic!("controller_send channel closed but drone isn't crashed")
        }
    }
}

#[cfg(test)]
mod drone_tests {
    use super::*;
    use crossbeam_channel::unbounded;

    fn get_test_drone() -> RustAndFurious {
        let id = rand::random::<u8>();
        let (controller_send, _) = unbounded();
        let (_, controller_recv) = unbounded();
        let (_, packet_recv) = unbounded();
        let packet_send = HashMap::new();
        let pdr = 1.0;
        RustAndFurious::new(id, controller_send, controller_recv, packet_recv, packet_send, pdr)
    }

    #[test]
    fn test_get_backwards_source_routing_header() {
        let drone = get_test_drone();

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
}