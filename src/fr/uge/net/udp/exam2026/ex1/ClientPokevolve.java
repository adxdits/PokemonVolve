package fr.uge.net.udp.exam2026.ex1;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.logging.Logger;

public class ClientPokevolve {
    private static final Logger logger = Logger.getLogger(ClientPokevolve.class.getName());
    private final InetSocketAddress serverAddress;
    private final DatagramChannel datagramChannel;
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final ByteBuffer recBuffer = ByteBuffer.allocate(2028);
    

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

  
   
    public ClientPokevolve(InetSocketAddress serverAddress) throws IOException {
        this.serverAddress = serverAddress;
        this.datagramChannel = DatagramChannel.open();
    }

    public void launch(String Pokemon) throws IOException {
    	datagramChannel.bind(null);
    	encode(sendBuffer, Pokemon);
    	sendBuffer.flip();
    	datagramChannel.send(sendBuffer, serverAddress);
    	
    	recBuffer.clear();
    	var sender = datagramChannel.receive(recBuffer);
    	recBuffer.flip();
    	var packet = decode(recBuffer);
    	if (packet.isEmpty() || !packet.get().pokemon().equals(Pokemon)) {
    	    logger.info("received packet for wrong pokemon, ignoring");
    	    return;
    	}
    	packet.ifPresent(System.out::println);
       
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
        new ClientPokevolve(server).launch(pokemon);
    }
}
