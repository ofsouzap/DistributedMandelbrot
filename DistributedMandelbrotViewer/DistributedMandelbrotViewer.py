import numpy as np;
from matplotlib import pyplot as plt;
from matplotlib import cm as colormap;
import socket;
from struct import pack, unpack;
from typing import Tuple as tTuple;

CHUNK_WIDTH = 4096;

MIN_AXIS = -2;
MAX_AXIS = 2;

REQUEST_ACCEPT_CODE = 0x00;
REQUEST_REJECT_CODE = 0x01;
REQUEST_NOT_AVAILABLE_CODE = 0x02;

TICK_COUNT = 6;

def recv_sock_msg_by_len(s: socket.socket,
    n: int) -> bytearray:

    out = bytearray();

    while len(out) < n:

        x = s.recv(n - len(out));

        if not x:
            raise Exception("EOF reached when trying to read socket message");

        out.extend(x);

    return out;

def deserialize_rle(data: bytearray) -> bytearray:

    out = bytearray();

    i = 0;

    while i < len(data):

        run_data = data[i:i+5];
        i += 5;

        count, val = unpack("IB", run_data);

        out.extend([val] * count);
    
    return out;

def chunk_data_to_value_array(raw_data: bytearray) -> bytearray:

    serialization_type_code = raw_data[0];

    data_body = raw_data[1:];

    if serialization_type_code == 0: return data_body; # Raw data
    elif serialization_type_code == 1: return deserialize_rle(data_body); # RLE
    else: raise Exception("Unknown serialization type code"); # Unknown

def get_chunk(srv_addr: str,
    srv_port: int,
    level: int,
    index_real: int,
    index_imag: int) -> np.ndarray:

    s = socket.socket();

    s.connect((srv_addr, srv_port));

    # Send request parameters

    s.send(pack("III", level, index_real, index_imag));

    # Get status

    status = unpack("B", s.recv(1))[0];

    if status == REQUEST_NOT_AVAILABLE_CODE:
        return None, False;
    elif status == REQUEST_REJECT_CODE:
        raise Exception("Request was rejected");
    elif status != REQUEST_ACCEPT_CODE:
        raise Exception("Unknown request status code: " + str(status));

    # Get response length

    resp_len = unpack("I", s.recv(4))[0];

    raw_data = recv_sock_msg_by_len(s, resp_len);

    # Convert raw data to value array

    data = chunk_data_to_value_array(raw_data);

    # Make numpy array from data

    if len(data) != CHUNK_WIDTH * CHUNK_WIDTH:
        raise Exception("Incorrect data length");

    vs = np.array(list(data), dtype=np.uint8);

    # Close and return

    s.close();

    return vs, True;

def data_to_img_array(data: np.ndarray) -> np.ndarray:

    assert data.ndim == 1;
    assert data.shape[0] == CHUNK_WIDTH * CHUNK_WIDTH;

    # Make 2D
    vs = data.reshape((CHUNK_WIDTH, CHUNK_WIDTH));

    # Floating-point normalise
    vs = vs.astype(float) / 256;

    # Invert values
    vs = 1 - vs;

    # Apply colormap
    colormapped = colormap.jet(vs).astype(float);

    BLACK = np.array((0, 0, 0, 1), dtype=float);

    # Reshape data to be used in the "np.where"
    val_data = vs.reshape((vs.shape[0], vs.shape[1], 1));

    # Make pixels black if in the Mandelbrot Set
    img = np.where(val_data == 1, BLACK, colormapped);

    return img;

def display_img(data: np.ndarray):

    assert data.ndim == 3;
    assert data.shape[2] == 4; #RGBA channels
    
    plt.imshow(data);
    plt.show();

def main():

    srv_addr = input("Server Addr> ");
    srv_port = int(input("Server Port> "));

    level = int(input("Level> "));
    index_real = int(input("Index Re> "));
    index_imag = int(input("Index Im> "));

    data, success = get_chunk(srv_addr = srv_addr,
        srv_port = srv_port,
        level = level,
        index_real = index_real,
        index_imag = index_imag);

    if not success:
        print("Chunk isn't available");
        return;

    img_data = data_to_img_array(data);

    display_img(img_data);

if __name__ == "__main__":
    main();
