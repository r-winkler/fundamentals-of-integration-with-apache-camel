package com.pluralsight.michaelhoffman.camel.customer.integration.addressupdateroute;

import com.pluralsight.michaelhoffman.camel.customer.integration.common.dto.Customer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpMethod;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.handler.advice.RequestHandlerRetryAdvice;
import org.springframework.integration.http.dsl.Http;
import org.springframework.messaging.Message;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


@Configuration
@EnableIntegration
public class AddressUpdatesToCustomerServiceRoute {

    @Value("${app.addressToCustomerRoute.directory}")
    private String sourcePath;

    @Value("${app.addressToCustomerRoute.moveDirectory}")
    private String movePath;

    @Value("${app.addressToCustomerRoute.includeFile}")
    private String filePattern;

    @Value("${app.customer-service.host}")
    private String uri;


    @Bean
    public IntegrationFlow flow() {

        return IntegrationFlows.from(Files.inboundAdapter(new File(sourcePath))
                                .autoCreateDirectory(true)
                                .filter(new SimplePatternFileListFilter(filePattern))
                                .filter(new AcceptOnceFileListFilter<>()),
                        c -> c.poller(Pollers.fixedRate(1000).maxMessagesPerPoll(1)))
                .log(Message::getPayload)
//                .wireTap(sf -> sf.handle(Files.outboundAdapter(new File(movePath))
//                        .fileExistsMode(FileExistsMode.REPLACE)
//                        .autoCreateDirectory(true)
//                        .deleteSourceFiles(true)))
                .transform(this::csvToCustomerList)
                .split()
                .log(Message::getPayload)
                .handle(it -> {
                    System.out.println("abc");
                    if (Math.random() > 0.1) {
                        throw new RuntimeException();
                    }
                    Http.outboundGateway(uri + "/" + ((Customer) it.getPayload()).getId()).httpMethod(HttpMethod.PATCH);
                }, e -> e.advice(retryAdvice()))
                .get();
    }


    @Bean
    public RequestHandlerRetryAdvice retryAdvice() {

        MaxAttemptsRetryPolicy maxAttemptsRetryPolicy = new MaxAttemptsRetryPolicy();
        maxAttemptsRetryPolicy.setMaxAttempts(3);

        ExponentialBackOffPolicy exponentialBackOffPolicy = new ExponentialBackOffPolicy();
        exponentialBackOffPolicy.setInitialInterval(2000);
        exponentialBackOffPolicy.setMultiplier(2.0);

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(maxAttemptsRetryPolicy);
        retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);

        RequestHandlerRetryAdvice advice = new RequestHandlerRetryAdvice();
        advice.setRetryTemplate(retryTemplate);
        return advice;
    }


    // a bit too complicated...probably there are simpler solutions...
    private List<Customer> csvToCustomerList(File file) {

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setNames("id",
                "addressLine1",
                "addressLine2",
                "city",
                "state",
                "postalCode");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        FlatFileItemReader<Customer> csvReader = new FlatFileItemReader<>();
        csvReader.setLinesToSkip(1); // skip header
        csvReader.setResource(new FileSystemResource(file.getAbsolutePath()));
        csvReader.setLineMapper(lineMapper);
        List<Customer> customers = new ArrayList<>();

        try {
            csvReader.open(new ExecutionContext());
            var line = csvReader.read();
            while (line != null) {
                customers.add(line);
                line = csvReader.read();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            customers = new ArrayList<>();
        }
        finally {
            csvReader.close();
            return customers;
        }

    }


}
