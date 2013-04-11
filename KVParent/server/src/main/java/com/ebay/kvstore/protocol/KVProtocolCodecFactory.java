package com.ebay.kvstore.protocol;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

import com.ebay.kvstore.protocol.decoder.DecoderManager;
import com.ebay.kvstore.protocol.decoder.DefaultProtocolDecoder;
import com.ebay.kvstore.protocol.decoder.DeleteRequestDecoder;
import com.ebay.kvstore.protocol.decoder.DeleteResponseDecoder;
import com.ebay.kvstore.protocol.decoder.GetRequestDecoder;
import com.ebay.kvstore.protocol.decoder.GetResponseDecoder;
import com.ebay.kvstore.protocol.decoder.IncrRequestDecoder;
import com.ebay.kvstore.protocol.decoder.IncrResponseDecoder;
import com.ebay.kvstore.protocol.decoder.RegionTableRequestDecoder;
import com.ebay.kvstore.protocol.decoder.RegionTableResponseDecoder;
import com.ebay.kvstore.protocol.decoder.SetRequestDecoder;
import com.ebay.kvstore.protocol.decoder.SetResponseDecoder;
import com.ebay.kvstore.protocol.decoder.StatRequestDecoder;
import com.ebay.kvstore.protocol.decoder.StatResponseDecoder;
import com.ebay.kvstore.protocol.encoder.DefaultProtocolEncoder;
import com.ebay.kvstore.protocol.encoder.DeleteRequestEncoder;
import com.ebay.kvstore.protocol.encoder.DeleteResponseEncoder;
import com.ebay.kvstore.protocol.encoder.EncoderManager;
import com.ebay.kvstore.protocol.encoder.GetRequestEncoder;
import com.ebay.kvstore.protocol.encoder.GetResponseEncoder;
import com.ebay.kvstore.protocol.encoder.IncrRequestEncoder;
import com.ebay.kvstore.protocol.encoder.IncrResponseEncoder;
import com.ebay.kvstore.protocol.encoder.RegionTableRequestEncoder;
import com.ebay.kvstore.protocol.encoder.RegionTableResponseEncoder;
import com.ebay.kvstore.protocol.encoder.SetRequestEncoder;
import com.ebay.kvstore.protocol.encoder.SetResponseEncoder;
import com.ebay.kvstore.protocol.encoder.StatRequestEncoder;
import com.ebay.kvstore.protocol.encoder.StatResponseEncoder;

public class KVProtocolCodecFactory implements ProtocolCodecFactory {
	private EncoderManager encoder;
	private DecoderManager decoder;

	public KVProtocolCodecFactory() {
		encoder = new EncoderManager(new DefaultProtocolEncoder());
		encoder.addEncoder(IProtocolType.Get_Req, new GetRequestEncoder());
		encoder.addEncoder(IProtocolType.Get_Resp, new GetResponseEncoder());
		encoder.addEncoder(IProtocolType.Set_Req, new SetRequestEncoder());
		encoder.addEncoder(IProtocolType.Set_Resp, new SetResponseEncoder());
		encoder.addEncoder(IProtocolType.Delete_Req, new DeleteRequestEncoder());
		encoder.addEncoder(IProtocolType.Delete_Resp, new DeleteResponseEncoder());
		encoder.addEncoder(IProtocolType.Stat_Req, new StatRequestEncoder());
		encoder.addEncoder(IProtocolType.Stat_Resp, new StatResponseEncoder());
		encoder.addEncoder(IProtocolType.Incr_Req, new IncrRequestEncoder());
		encoder.addEncoder(IProtocolType.Incr_Resp, new IncrResponseEncoder());
		encoder.addEncoder(IProtocolType.Region_Table_Req, new RegionTableRequestEncoder());
		encoder.addEncoder(IProtocolType.Region_Table_Resp, new RegionTableResponseEncoder());

		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		decoder = new DecoderManager(new DefaultProtocolDecoder(loader));
		decoder.addDecoder(IProtocolType.Get_Req, new GetRequestDecoder());
		decoder.addDecoder(IProtocolType.Get_Resp, new GetResponseDecoder());
		decoder.addDecoder(IProtocolType.Set_Req, new SetRequestDecoder());
		decoder.addDecoder(IProtocolType.Set_Resp, new SetResponseDecoder());
		decoder.addDecoder(IProtocolType.Delete_Req, new DeleteRequestDecoder());
		decoder.addDecoder(IProtocolType.Delete_Resp, new DeleteResponseDecoder());
		decoder.addDecoder(IProtocolType.Stat_Req, new StatRequestDecoder());
		decoder.addDecoder(IProtocolType.Stat_Resp, new StatResponseDecoder());
		decoder.addDecoder(IProtocolType.Incr_Req, new IncrRequestDecoder());
		decoder.addDecoder(IProtocolType.Incr_Resp, new IncrResponseDecoder());
		decoder.addDecoder(IProtocolType.Region_Table_Req, new RegionTableRequestDecoder());
		decoder.addDecoder(IProtocolType.Region_Table_Resp, new RegionTableResponseDecoder());
	}

	@Override
	public ProtocolEncoder getEncoder(IoSession session) throws Exception {
		return encoder;
	}

	@Override
	public ProtocolDecoder getDecoder(IoSession session) throws Exception {
		return decoder;
	}

}
