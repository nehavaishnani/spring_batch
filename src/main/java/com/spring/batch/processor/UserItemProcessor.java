package com.spring.batch.processor;

import com.spring.batch.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

public class UserItemProcessor implements ItemProcessor<User, User> {

    private static final Logger log = LoggerFactory.getLogger(UserItemProcessor.class);

    @Override
    public User process(final User user) throws Exception {
        final String firstName = user.getFirstName().toUpperCase();
        final String lastName = user.getLastName().toUpperCase();
        final String contact = user.getContact();

        final User transformeduser = new User(firstName, lastName, contact);

        log.info("Converting (" + user + ") into (" + transformeduser + ")");

        return transformeduser;
    }

}