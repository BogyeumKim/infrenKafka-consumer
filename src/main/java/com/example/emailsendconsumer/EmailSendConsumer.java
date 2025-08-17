package com.example.emailsendconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class EmailSendConsumer {

    @KafkaListener(
            topics = "email.send",
            groupId = "email-send-group",
            concurrency = "3" // 멀티쓰레드를 활용해서 병렬 처리할 파티션 개수
    )
    @RetryableTopic(
            attempts = "5", // 총 5번까지 재시작
            // 1초 간격 * multiplier 2 => 1초 .. 2초 .. 4초.. 8초 .. 16초 .. 순서로 설정됨
            // 현업에서는 재시도를 3~4회로 설정해둔다고함.
            backoff = @Backoff(delay = 1000, multiplier = 2),

            //email.send-dlt 로 기본 생성된다 suffix 를 .dlt 로 설정하여 email.send.dlt 로 생성되도록 설정한다.
            dltTopicSuffix = ".dlt"
    )
    public void consume(String message) {
        System.out.println("Kafka로 부터 받아온 메세지 : " + message);

        EmailSendMessage emailSendMessage = EmailSendMessage.fromJson(message);

        if ( emailSendMessage.getTo().equals("fail@naver.com")) {
            System.out.println("잘못된 이메일 주소로 인해 발송 실패");
            throw new RuntimeException("잘못된 이메일 주소로 인해 발송 실패!");
        }

        // .. 실제 이메일 발송 로직 생략 ..
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException("이메일 발송 실패");
        }

        System.out.println("이메일 발송 완료");
    }
}
