package fr.uge.net.udp.exam2026.ex1;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;



public class ClientPokevolve2 {
    private static final Logger logger = Logger.getLogger(ClientPokevolve2.class.getName());
    private static final int TIMEOUT = 300;
    private final InetSocketAddress serverAddress;
    private final DatagramChannel datagramChannel;
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final ByteBuffer recBuffer = ByteBuffer.allocate(2048);
    

   private record Packet(String pokemon, String evolution) {
	   @Override
	   public String toString() {
		return "[" + pokemon + " , " + evolution + "]";}
   };
   
   private void encode(ByteBuffer buffer, String pokemon) {
	   buffer.clear();
	   var name = StandardCharsets.UTF_8.encode(pokemon);
	   var size = name.remaining();
	   if (buffer.remaining() < Integer.BYTES + size) {
		   logger.warning("buffer is too small closing...");
		   return ;
	   }
	   buffer.putInt(size).put(name);
   }

   private Optional<Packet> decode(ByteBuffer buffer) {
	   if (buffer.remaining() < Integer.BYTES) {
		   logger.info("packet is too small");
		   return Optional.empty();
	   }

	   var totalSize = buffer.remaining();
	   var pokemonSize = buffer.getInt(buffer.position() + totalSize - Integer.BYTES);
	   var evolutionSize = totalSize - pokemonSize - Integer.BYTES;

	   var savedLimit = buffer.limit();
	   buffer.limit(buffer.position() + pokemonSize);
	   var pokemonName = StandardCharsets.UTF_8.decode(buffer).toString();

	   buffer.limit(buffer.position() + evolutionSize);
	   var evolution = StandardCharsets.UTF_8.decode(buffer).toString();

	   return Optional.of(new Packet(pokemonName, evolution));
   }

   
    public ClientPokevolve2(InetSocketAddress serverAddress) throws IOException {
        this.serverAddress = serverAddress;
        this.datagramChannel = DatagramChannel.open();
    }

    public void launch(String pokemon) throws IOException {
        datagramChannel.bind(null);
        var evolutions = new ArrayList<String>();
        var seen = new HashSet<String>();
        evolutions.add(pokemon);
        seen.add(pokemon);
        var current = pokemon;

        var queue = new ArrayBlockingQueue<Packet>(1);

        var listener = Thread.ofPlatform().daemon(true).start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                recBuffer.clear();
                try {
                    datagramChannel.receive(recBuffer);
                } catch (ClosedChannelException e) {
                    logger.info("channel is closed, closing...");
                    return;
                } catch (IOException e) {
                    logger.severe("listener io exception, stopping...");
                    return;
                }
                recBuffer.flip();
                var packet = decode(recBuffer);
                if (packet.isEmpty()) {
                    continue;
                }
                queue.offer(packet.get());
            }
        });

        while (true) {
            encode(sendBuffer, current);
            sendBuffer.flip();
            datagramChannel.send(sendBuffer, serverAddress);

            Packet response = null;
            while (response == null) {
                try {
                    response = queue.poll(TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.info("interrupted, closing...");
                    datagramChannel.close();
                    return;
                }
                if (response == null) {
                    // timeout, resend
                    sendBuffer.rewind();
                    datagramChannel.send(sendBuffer, serverAddress);
                    continue;
                }
                if (!response.pokemon().equals(current)) {
                    // wrong pokemon, ignore and wait again
                    response = null;
                }
            }

            if (response.pokemon().equals(response.evolution())) {
                break;
            }
            if (!seen.add(response.evolution())) {
                break;
            }
            evolutions.add(response.evolution());
            current = response.evolution();
        }

        System.out.println(evolutions);
        datagramChannel.close();
    }

    public static void usage() {
        System.out.println("Usage: Client host port pokename");
    }

    public static void main(String[] args) throws IOException {
       if (args.length < 3) {
            usage();
            return;
        }
        var server = new InetSocketAddress(args[0], Integer.parseInt(args[1]));
        var pokemon = args[2];
        new ClientPokevolve2(server).launch(pokemon);
    }
}