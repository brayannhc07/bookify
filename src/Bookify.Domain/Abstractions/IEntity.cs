namespace Bookify.Domain.Abstractions;

public interface IEntity {
    IEnumerable<IDomainEvent> GetDomainEvents();
    void ClearDomainEvents();
}