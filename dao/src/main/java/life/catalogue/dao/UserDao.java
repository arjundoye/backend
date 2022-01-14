package life.catalogue.dao;

import life.catalogue.api.event.UserChanged;
import life.catalogue.api.exception.NotFoundException;
import life.catalogue.api.model.Page;
import life.catalogue.api.model.ResultPage;
import life.catalogue.api.model.User;
import life.catalogue.api.util.ObjectUtils;
import life.catalogue.db.mapper.DatasetMapper;
import life.catalogue.db.mapper.UserMapper;

import java.time.LocalDateTime;
import java.util.*;

import javax.annotation.Nullable;
import javax.validation.Validator;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.EventBus;

public class UserDao extends EntityDao<Integer, User, UserMapper> {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(UserDao.class);

  private final EventBus bus;

  public UserDao(SqlSessionFactory factory, EventBus bus, Validator validator) {
    super(true, factory, User.class, UserMapper.class, validator);
    this.bus = bus;
  }

  public ResultPage<User> search(@Nullable final String q, @Nullable Page page) {
    page = page == null ? new Page() : page;
    try (SqlSession session = factory.openSession()){
      UserMapper um = session.getMapper(mapperClass);
      List<User> result = um.search(q, defaultPage(page));
      return new ResultPage<>(page, result, () -> um.searchCount(q));
    }
  }

  public void updateSettings(Map<String, String> settings, User user) {
    if (user != null && settings != null) {
      user.setSettings(settings);
      try (SqlSession session = factory.openSession()){
        session.getMapper(UserMapper.class).update(user);
      }
    }
  }

  public void changeRole(int key, User admin, List<User.Role> roles) {
    User user;
    final var newRoles = new HashSet<User.Role>(ObjectUtils.coalesce(roles, Collections.EMPTY_SET));
    try (SqlSession session = factory.openSession()) {
      var dm = session.getMapper(DatasetMapper.class);
      user = session.getMapper(mapperClass).get(key);
      if (user == null) {
        throw NotFoundException.notFound(User.class, key);
      }

      // if we revoke the editor or reviewer role the user will lose access to all datasets
      if (user.hasRole(User.Role.EDITOR) && !newRoles.contains(User.Role.EDITOR)) {
        user.getEditor().forEach(dk -> {
          dm.removeEditor(dk, user.getKey(), admin.getKey());
        });
      }
      if (user.hasRole(User.Role.REVIEWER) && !newRoles.contains(User.Role.REVIEWER)) {
        user.getReviewer().forEach(dk -> {
          dm.removeReviewer(dk, user.getKey(), admin.getKey());
        });
      }
    }
    // only update if changed
    if (!user.getRoles().equals(newRoles)) {
      user.setRoles(newRoles);
      update(user, admin.getKey());
    }
  }

  public void block(int key, User admin) {
    try (SqlSession session = factory.openSession()){
      session.getMapper(UserMapper.class).block(key, LocalDateTime.now());
    }
  }

  public void unblock(int key, User admin) {
    try (SqlSession session = factory.openSession()){
      session.getMapper(UserMapper.class).block(key, null);
    }
  }

  private static Page defaultPage(Page page){
    return page == null ? new Page(0, 10) : page;
  }

  @Override
  protected boolean createAfter(User obj, int user, UserMapper mapper, SqlSession session) {
    session.close();
    bus.post(UserChanged.created(obj));
    return false;
  }

  @Override
  protected boolean updateAfter(User obj, User old, int user, UserMapper mapper, SqlSession session, boolean keepSessionOpen) {
    if (!keepSessionOpen) {
      session.close();
    }
    bus.post(UserChanged.changed(obj));
    return keepSessionOpen;
  }

  @Override
  protected boolean deleteAfter(Integer key, User old, int user, UserMapper mapper, SqlSession session) {
    bus.post(UserChanged.deleted(old));
    return false;
  }
}
