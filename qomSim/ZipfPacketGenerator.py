from functools import partial
from socket import PACKET_LOOPBACK
from struct import pack
from ns.packet.dist_generator import DistPacketGenerator
from ns.packet.packet import Packet
from utils import generate_zipf
from MonitorData import MonitorData
import random

class ZipfPacketGenerator(DistPacketGenerator):
    def __init__(self,
                 env,
                 element_id,
                 arrival_dist,
                 size_dist,
                 initial_delay=0,
                 finish=float("inf"),
                 flow_id=0,
                 rec_flow=False,
                 debug=False,
                 alpha = 2,
                 begin = 10000,
                 end = 100000):
        st, flow_size = generate_zipf(alpha, begin, end)
        if flow_size < 50:
            my_arrival_dist = arrival_dist
        else:
            my_arrival_dist = partial(random.expovariate, flow_size / (100000 - st))
        super().__init__(env, element_id, my_arrival_dist, size_dist, initial_delay, finish, flow_id, rec_flow, debug)
        self.initial_delay = st
        self.flow_size = flow_size

    def run(self):
        """The generator function used in simulations."""
        yield self.env.timeout(self.initial_delay)
        while self.packets_sent < self.flow_size and self.env.now < self.finish:
            # wait for next transmission
            yield self.env.timeout(self.arrival_dist())

            self.packets_sent += 1
            packet = Packet(self.env.now,
                            self.size_dist(),
                            self.packets_sent,
                            src=self.element_id,
                            flow_id=self.flow_id)
            if self.rec_flow:
                self.time_rec.append(packet.time)
                self.size_rec.append(packet.size)

            if self.debug:
                print(
                    f"Sent packet {packet.packet_id} with flow_id {packet.flow_id} at "
                    f"time {self.env.now}.")
            if packet.flow_id in MonitorData.trueFlowFrequent:
                MonitorData.trueFlowFrequent[packet.flow_id] += 1
            else:
                MonitorData.trueFlowFrequent[packet.flow_id] = 1
            
            if packet.flow_id in MonitorData.thisSegmentTrueFlows:
                MonitorData.thisSegmentTrueFlows[packet.flow_id] += 1
            else:
                MonitorData.thisSegmentTrueFlows[packet.flow_id] = 1

            self.out.put(packet)
