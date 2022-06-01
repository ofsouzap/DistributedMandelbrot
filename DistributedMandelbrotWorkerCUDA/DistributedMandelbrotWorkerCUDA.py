import socket;
from struct import pack, unpack;
from numba import cuda;
from numba import vectorize;
import numpy as np;

MIN_AXIS = -2;
MAX_AXIS = 2;

REQUEST_CODE = 0x00;
RESPONSE_CODE = 0x01;

WORKLOAD_AVAILABLE_CODE = 0x10;
WORKLOAD_NOT_AVAILABLE_CODE = 0x11;

WORKLOAD_ACCEPT_CODE = 0x20;
WORKLOAD_REJECT_CODE = 0x21;

def gen_arrays(start_r: np.float64,
    start_i: np.float64,
    _range: np.float64,
    definition: np.int32 = 4096):

    r = np.linspace(
        start = start_r,
        stop = start_r + _range,
        num = definition);

    i = np.linspace(
        start = start_i,
        stop = start_i + _range,
        num = definition);

    r_rep = np.tile(r, definition);
    i_rep = np.repeat(i, definition);

    return r_rep, i_rep;

@vectorize(["int32(float64, float64, int32)"], target = "cuda")
def calc_mb_value(r: np.float64,
    i: np.float64,
    mrd: np.int32) -> np.int32:

    c = (r, i);
    z = (r,i);

    for i in range(1, mrd):

        # Square the complex number
        z = (
            z[0]*z[0] - z[1]*z[1],
            2 * z[0] * z[1]
        );

        # Add c
        z = (
            z[0] + c[0],
            z[1] + c[1]
        );

        # Get the square of the magnitude
        sqr_mag = z[0] * z[0] + z[1] * z[1];

        # Compare to the square of 2 (aka 4)
        if (sqr_mag >= 4):
            return i;
    
    return 0;

def process_workload(level: int,
    index_real: int,
    index_imag: int) -> np.ndarray:

    chunk_range = (MAX_AXIS - MIN_AXIS) / level;

    start_r = MIN_AXIS + (chunk_range * index_real);
    start_i = MIN_AXIS + (chunk_range * index_imag);

    mrd = 1024;
    definition = 4096; # Chunk size

    r, i = gen_arrays(start_r = start_r,
        start_i = start_i,
        _range = chunk_range,
        definition = definition);

    r_device = cuda.to_device(r);
    i_device = cuda.to_device(i);

    out_device = cuda.device_array(shape=(definition*definition,), dtype=np.int32);

    calc_mb_value(r_device, i_device, mrd, out=out_device);

    out = out_device.copy_to_host();

    out = (out.astype(np.float64) * 256) / mrd;

    out = out.astype(np.uint8);

    return out;

def receive_workload(sock: socket.socket):

    level = unpack("I", sock.recv(4))[0];
    indexReal = unpack("I", sock.recv(4))[0];
    indexImag = unpack("I", sock.recv(4))[0];

    return level, indexReal, indexImag;

def do_workload_single(addr: str, port: int) -> bool:

    # Receive workload

    sock = socket.socket();
    
    sock.connect((addr, port));

    sock.send(pack("B", REQUEST_CODE));

    response = sock.recv(1)[0];

    if response == WORKLOAD_AVAILABLE_CODE:
        print("Workload was available, receiving");
        workload = receive_workload(sock);
        
    elif response == WORKLOAD_NOT_AVAILABLE_CODE:
        print("No workload was available, ending program");
        return False;
    
    else:
        raise Exception("Unknown response code to request: " + str(response));

    sock.close();

    print("Workload received:", workload);

    # Perform calculation

    print("Starting calculation...");

    out = process_workload(workload[0], workload[1], workload[2]);

    print("Calculation complete");

    # Return completed workload

    sock = socket.socket();
    
    sock.connect((addr, port));

    sock.send(pack("B", RESPONSE_CODE));

    sock.send(pack("III", workload[0], workload[1], workload[2]));

    response = sock.recv(1)[0];

    if response == WORKLOAD_ACCEPT_CODE:
        print("Response accepted");

    elif response == WORKLOAD_REJECT_CODE:
        print("Response rejected, ending program");
        return True;

    else:
        raise Exception("Unknown response code to request: " + str(response));

    sock.send(out.tobytes());

    print("Sent response");

    sock.close();

    print("Process complete");

    return True;

def main():

    addr = input("Server Addr> ");
    port = int(input("Server Port> "));
    
    while do_workload_single(addr, port):
        pass;

if __name__ == "__main__":
    main();
