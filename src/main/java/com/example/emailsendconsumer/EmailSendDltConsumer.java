package com.example.emailsendconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class EmailSendDltConsumer {

    @KafkaListener(
            topics = "email.send.dlt",
            groupId = "email-send-dlt-group"
    )
    public void consume(String message) {

        // 로그 시스템에 전송
        System.out.println("로그 시스템에 전송 : " + message);

        // 알림 발송
        System.out.println("Slack에 알림 발송");
    }
}
