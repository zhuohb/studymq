package com.zhb.broker.netty.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import com.zhb.common.coder.TcpMsgDecoder;
import com.zhb.common.coder.TcpMsgEncoder;
import com.zhb.common.constants.TcpConstants;
import com.zhb.common.event.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author idea
 * @Date: Created in 18:02 2024/6/15
 * @Description
 */
public class BrokerServer {

    private static Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    private int port;

    public BrokerServer(int port) {
        this.port = port;
    }


    /**
     * 启动服务端程序
     *
     * @throws InterruptedException
     */
    public void startServer() throws InterruptedException {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ByteBuf delimiter = Unpooled.copiedBuffer(TcpConstants.DEFAULT_DECODE_CHAR.getBytes());
                ch.pipeline().addLast(new DelimiterBasedFrameDecoder(1024 * 8, delimiter));
                ch.pipeline().addLast(new TcpMsgDecoder());
                ch.pipeline().addLast(new TcpMsgEncoder());
                ch.pipeline().addLast(new BrokerServerHandler(new EventBus("broker-server-handle")));
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            logger.info("broker is closed");
        }));
        ChannelFuture channelFuture = bootstrap.bind(port).sync();
        logger.info("start nameserver application on port:{}", port);
        //阻塞代码
        channelFuture.channel().closeFuture().sync();
    }
}
